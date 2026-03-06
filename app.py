from flask import Flask, render_template, request, jsonify
from scraper import KeibaLabScraper
from database_manager import DatabaseManager
import datetime
import os
import sys
import subprocess
import json
import re
import threading
import time
import traceback
import concurrent.futures

# 結果分析プロジェクトのパスを通す
ANALYZER_DIR = r"c:\Users\user\OneDrive\ドキュメント\testapp_antigra\keiba_result_analyzer"
sys.path.append(ANALYZER_DIR)

app = Flask(__name__)
scraper = KeibaLabScraper()
db = DatabaseManager()

# 予測結果キャッシュ (メモリ節約のため直近数件程度)
PREDICTION_CACHE = {}
CACHE_LOCK = threading.Lock()

def get_cached_prediction(race_id, force_refresh=False, extra_info=None, race_date=None):
    """予測結果を取得する（キャッシュがあればそれを使う。force_refresh=Trueで強制再取得）"""
    if not force_refresh:
        # 1. メモリキャッシュをチェック
        with CACHE_LOCK:
            if race_id in PREDICTION_CACHE:
                return PREDICTION_CACHE[race_id]
        
        # 2. SQLiteキャッシュをチェック
        cached = db.get_cached_analysis(race_id, force_refresh=force_refresh)
        if cached:
            with CACHE_LOCK:
                PREDICTION_CACHE[race_id] = cached
            return cached
    
    # 解析実行（重い処理なのでロックの外で行う）
    prediction = scraper.get_full_analysis(race_id, extra_info=extra_info)
    
    if prediction:
        # DBに保存
        if not race_date:
            # IDから日付(YYYYMMDD)を推測 (JRA形式)
            race_date = race_id[:8] if len(race_id) >= 8 else datetime.date.today().strftime('%Y%m%d')
        db.save_analysis(race_id, race_date, prediction)

        with CACHE_LOCK:
            PREDICTION_CACHE[race_id] = prediction
            # キャッシュが大きくなりすぎないように制限 (例: 100件)
            if len(PREDICTION_CACHE) > 100:
                first_key = next(iter(PREDICTION_CACHE))
                del PREDICTION_CACHE[first_key]
            
    return prediction

@app.route('/api/clear_cache')
def clear_cache():
    """予測キャッシュをすべてクリアする"""
    with CACHE_LOCK:
        count = len(PREDICTION_CACHE)
        PREDICTION_CACHE.clear()
        # スクレイパーの内部キャッシュもクリア
        if hasattr(scraper, 'clear_internal_cache'):
            scraper.clear_internal_cache()
        # DBも物理削除
        db_cleared = db.clear_cache()
        msg = f'キャッシュをクリアしました ({count}件)'
        if db_cleared:
            msg += ' & データベースを初期化しました'
        return jsonify({'status': 'success', 'message': msg})

@app.route('/api/clear_cache_by_date')
def clear_cache_by_date():
    """指定日付のキャッシュを削除する"""
    date_str = request.args.get('date')
    if not date_str or not date_str.isdigit() or len(date_str) != 8:
        return jsonify({'error': '有効な日付 (YYYYMMDD) を指定してください'}), 400

    # メモリキャッシュからも該当日のデータを削除
    with CACHE_LOCK:
        mem_deleted = 0
        keys_to_del = [k for k in PREDICTION_CACHE.keys() if k.startswith(date_str)]
        for k in keys_to_del:
            del PREDICTION_CACHE[k]
            mem_deleted += 1

    # スクレイパーの内部キャッシュもクリア (特定日のみの削除でも念のため全体をクリアするか、該当馬のみ特定が必要だが、全体クリアが安全)
    if hasattr(scraper, 'clear_internal_cache'):
        scraper.clear_internal_cache()

    # SQLiteキャッシュから削除
    deleted = db.clear_cache_by_date(date_str)
    if deleted is None:
        return jsonify({'status': 'error', 'message': 'キャッシュ削除中にエラーが発生しました'}), 500

    formatted_date = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"
    total = deleted['races'] + deleted['analysis'] + deleted['results'] + mem_deleted
    msg = f'{formatted_date} のキャッシュを削除しました (レース一覧:{deleted["races"]}件, 分析:{deleted["analysis"]}件, 結果:{deleted["results"]}件, メモリ:{mem_deleted}件)'
    return jsonify({'status': 'success', 'message': msg, 'deleted': deleted, 'total': total})

@app.route('/api/cache_stats')
def cache_stats():
    """キャッシュの日付別統計を返す"""
    stats = db.get_cache_stats()
    today = datetime.date.today().strftime('%Y%m%d')
    # 各日付に「結果あり/なし」のフラグを付与
    for s in stats:
        s['has_results'] = s['result_count'] > 0
        s['is_past'] = s['date'] < today
        s['is_today'] = s['date'] == today
    return jsonify({'status': 'success', 'stats': stats, 'today': today})

@app.route('/')
def index():
    today = datetime.date.today().strftime("%Y%m%d")
    return render_template('index.html', default_date=today)

@app.route('/api/races')
def get_races():
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({'error': 'Date is required'}), 400
    
    # Simple validation
    if not date_str.isdigit() or len(date_str) != 8:
        return jsonify({'error': 'Invalid date format (YYYYMMDD)'}), 400

    try:
        force_refresh = request.args.get('force') == '1'
        skip_scoring = request.args.get('skip_scoring') == '1'

        # SQLiteキャッシュをチェック
        if not force_refresh:
            cached = db.get_cached_races(date_str, force_refresh=force_refresh)
            if cached:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Cache Hit: Races for {date_str}")
                # 既に予測キャッシュがあるものは自信度を動的に付与
                if not skip_scoring:
                    # 既に予測キャッシュがあるものは自信度を動的に付与
                    for race in cached.get('races', []):
                        rid = race.get('id')
                        if rid:
                            p = get_cached_prediction(rid)
                            if p:
                                race['confidence'] = p.get('confidence', 0)
                
                cached['is_protected'] = scraper.is_protected
                cached['from_cache'] = True
                return jsonify(cached)

        res = scraper.get_races_by_date(date_str, skip_scoring=skip_scoring)
        
        # 保存前に、既に予測キャッシュがあるものは自信度を付与
        if not skip_scoring:
            # 保存前に、既に予測キャッシュがあるものは自信度を付与
            for race in res.get('races', []):
                rid = race.get('id')
                if rid:
                    p = get_cached_prediction(rid)
                    if p:
                        race['confidence'] = p.get('confidence', 0)
        
        # 保存
        db.save_races(date_str, res)

        # 保護モードの状態を付与
        res['is_protected'] = scraper.is_protected
        return jsonify(res)
    except Exception as e:
        import traceback
        print(f"Races fetch error: {e}")
        print(traceback.format_exc())
        return jsonify({'error': str(e), 'status': 'error', 'is_protected': scraper.is_protected}), 500

@app.route('/api/score_race')
def get_score_race():
    race_id = request.args.get('race_id')
    if not race_id:
        return jsonify({'error': 'Race ID is required'}), 400
    
    try:
        # 1. 指標を取得
        inds = scraper._fetch_race_indicators(race_id)
        
        # 2. レース情報を取得 (スコアリングに必要)
        info = scraper.scrape_race_info(race_id)
        
        # 3. スコア計算
        stable_score, stable_reasons = scraper._calculate_stability(info, inds)
        rough_score, rough_reasons = scraper._calculate_roughness(info, inds)
        
        return jsonify({
            'id': race_id,
            'stable_score': stable_score,
            'stable_reasons': stable_reasons,
            'rough_score': rough_score,
            'rough_reasons': rough_reasons,
            'has_score': stable_score > 0 or rough_score > 0
        })
    except Exception as e:
        print(f"Score race error: {e}")
        return jsonify({'error': str(e), 'is_protected': scraper.is_protected}), 500

@app.route('/api/race_details')
def get_race_details():
    race_id = request.args.get('race_id')
    if not race_id:
        return jsonify({'error': 'Race ID is required'}), 400
        
    try:
        links = scraper.get_race_details(race_id)
        return jsonify(links)
    except Exception as e:
        print(f"Race details error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/horse_data')
def get_horse_data():
    horse_id = request.args.get('horse_id')
    if not horse_id:
        return jsonify({'error': 'Horse ID is required'}), 400
        
    try:
        links = scraper.get_horse_data(horse_id)
        return jsonify(links)
    except Exception as e:
        print(f"Horse data error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze_race')
def analyze_race():
    race_id = request.args.get('race_id')
    force_refresh = request.args.get('force_refresh') == '1'
    
    # 追加情報のパース
    extra_info_json = request.args.get('extra_info')
    extra_info = {}
    if extra_info_json:
        try:
            extra_info = json.loads(extra_info_json)
            print(f"DEBUG extra_info parsed ok: {extra_info}")
        except Exception as e:
            print(f"DEBUG extra_info parse error: {e}")
    else:
        print("DEBUG extra_info_json is empty")

    if not race_id:
        return jsonify({'error': 'Race ID is required'}), 400
    
    try:
        data = get_cached_prediction(race_id, force_refresh=force_refresh, extra_info=extra_info)
        return jsonify({
            'status': 'success',
            'analysis': data,
            'is_protected': scraper.is_protected
        })
    except Exception as e:
        import traceback
        print(f"Analysis error: {e}\n{traceback.format_exc()}")
        return jsonify({'error': str(e), 'is_protected': scraper.is_protected}), 500

@app.route('/api/batch_race_urls')
def batch_race_urls():
    """Returns all race detail URLs for a given date."""
    date_str = request.args.get('date')
    if not date_str or not date_str.isdigit() or len(date_str) != 8:
        return jsonify({'error': 'Valid date (YYYYMMDD) is required'}), 400
    
    res = scraper.get_races_by_date(date_str)
    races = res.get('races', [])
    all_urls = []
    for race in races:
        details = scraper.get_race_details(race['id'])
        all_urls.append({
            'race_name': race['name'],
            'race_id': race['id'],
            'urls': details
        })
    return jsonify({'batch': all_urls})

@app.route('/api/race_horses')
def race_horses():
    """Scrapes umabashira for a race and returns horse list with data URLs."""
    race_id = request.args.get('race_id')
    if not race_id:
        return jsonify({'error': 'Race ID is required'}), 400
    
    try:
        horses = scraper.scrape_race_card(race_id)
        # Add data URLs for each horse
        for horse in horses:
            hid = horse['horse_id']
            horse['history_url'] = f"{scraper.BASE_URL}/db/horse/{hid}/"
            horse['condition_url'] = f"{scraper.BASE_URL}/db/horse/{hid}/data.html"
        return jsonify({'horses': horses})
    except Exception as e:
        print(f"Race horses error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/run_backtest')
def run_backtest():
    """バックテストエンジンを実行して結果を返す"""
    try:
        from backtest_engine import BacktestEngine
        engine = BacktestEngine()
        # 最新20レースでテスト
        df, analysis = engine.run_backtest(limit=20)
        
        if df is None or df.empty:
            return jsonify({
                'status': 'empty', 
                'message': '判定可能な過去データがDBに見つかりませんでした（結果取得を行ってください）'
            })
            
        summary = {}
        target_marks = [
            ('mark_double_circle', '◎ 本命'), 
            ('mark_circle', '○ 対抗'), 
            ('mark_triangle', '▲ 単穴')
        ]
        
        for label, name in target_marks:
            # カラムが存在するかチェック
            hit_col = f'{label}_hit'
            win_col = f'{label}_win'
            pay_col = f'{label}_win_pay'
            
            if win_col in df.columns:
                summary[label] = {
                    'name': name,
                    'win_rate': round(df[win_col].mean() * 100, 1) if not df[win_col].empty else 0,
                    'hit_rate': round(df[hit_col].mean() * 100, 1) if not df[hit_col].empty else 0,
                    'win_roi': round((df[pay_col].sum() / len(df)) * 100, 1) if len(df) > 0 else 0
                }
            else:
                summary[label] = {'name': name, 'win_rate': 0, 'hit_rate': 0, 'win_roi': 0}
        
        return jsonify({
            'status': 'success',
            'summary': summary,
            'total_races': len(df),
            'analysis': analysis[:15] # 上位15件に絞って返す
        })
    except Exception as e:
        import traceback, datetime
        err_msg = f"検証エラー: {str(e)}"
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Exception Error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'status': 'error', 'message': err_msg}), 500


@app.route('/api/verify_date_predictions')
def verify_date_predictions():
    """指定した日の全レースについて、的中検証を行い逐次結果を返す (SSE/NDJSON)"""
    from flask import Response, stream_with_context
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({'error': 'Date is required'}), 400
        
    filters = {
        'keyword': request.args.get('keyword'),
        'surface': request.args.get('surface'),
        'venues': request.args.get('venues'),
        'race_nums': request.args.get('race_nums'),
        'horses': request.args.get('horses'),
        'shinba': request.args.get('shinba') == '1',
        'handicap': request.args.get('handicap') == '1',
        'grade': request.args.get('grade') == '1'
    }

    force = request.args.get('force') == '1'
    if force:
        with CACHE_LOCK:
            keys_to_del = [k for k in PREDICTION_CACHE.keys() if k.startswith(date_str)]
            for k in keys_to_del:
                del PREDICTION_CACHE[k]

    def generate():
        try:
            # 0. 準備開始を即座に通知
            yield json.dumps({'status': 'progress', 'type': 'preparing', 'message': 'レース一覧を取得中...'}) + '\n'
            
            # 1. その日の全レースIDを取得
            res = scraper.get_races_by_date(date_str, skip_scoring=True)
            races = res.get('races', [])
            if not races:
                yield json.dumps({'status': 'empty', 'message': '該当日のレースが見つかりません'}) + '\n'
                return

            # フィルタ適用 (ストリーミング前に一括で行う)
            target_races = []
            for r in races:
                is_match = True
                try:
                    r_num_str = r.get('race_num', '0')
                    r_num = int(r_num_str) if str(r_num_str).isdigit() else 0
                    h_count_str = r.get('horse_count', '0')
                    h_count = int(h_count_str) if str(h_count_str).isdigit() else 0
                    r_class = r.get('race_class', '')
                    r_name = r.get('name', '')
                    r_surf = r.get('surface', '')
                    
                    if filters.get('keyword'):
                        kw = filters['keyword']
                        if kw not in r_name and kw not in r_class: is_match = False
                    
                    venues_str = filters.get('venues')
                    if venues_str:
                        venue_list = venues_str.split(',')
                        if r.get('venue') not in venue_list: is_match = False
                    
                    f_surf = filters.get('surface')
                    if f_surf == 'turf' and '芝' not in r_surf: is_match = False
                    elif f_surf == 'dirt' and 'ダート' not in r_surf: is_match = False
                    elif f_surf == 'jump' and '障' not in r_surf: is_match = False
                    
                    if filters.get('shinba') and '新馬' not in r_class: is_match = False
                    if filters.get('handicap') and not r.get('is_handicap'): is_match = False
                    if filters.get('grade') and not any(g in r_class for g in ['G1', 'G2', 'G3', '重賞']): is_match = False
                    if filters.get('mare') and not r.get('is_mare'): is_match = False
                    
                    race_nums_str = filters.get('race_nums')
                    if race_nums_str:
                        race_num_list = [int(x) for x in race_nums_str.split(',') if x.isdigit()]
                        if r_num not in race_num_list: is_match = False
                    
                    f_horses = filters.get('horses')
                    if f_horses == 'lt10' and h_count > 10: is_match = False
                    elif f_horses == '12' and h_count != 12: is_match = False
                    elif f_horses == '12-14' and not (12 <= h_count <= 14): is_match = False
                    elif f_horses == 'gt15' and h_count < 15: is_match = False
                except Exception as fe:
                    print(f"Filter error in {r.get('id')}: {fe}")
                    is_match = False
                
                if is_match:
                    target_races.append(r)

            yield json.dumps({'status': 'progress', 'type': 'start', 'total': len(target_races)}) + '\n'

            from result_scraper import ResultScraper
            r_scraper = ResultScraper()
            all_results = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                future_to_race = {executor.submit(process_single_race_internal, race, date_str, r_scraper, scraper, PREDICTION_CACHE, CACHE_LOCK, get_cached_prediction): race for race in target_races}
                
                for future in concurrent.futures.as_completed(future_to_race):
                    res_item = future.result()
                    if res_item:
                        all_results.append(res_item)
                        yield json.dumps({'status': 'progress', 'type': 'race', 'data': res_item}) + '\n'
                    else:
                        race = future_to_race[future]
                        yield json.dumps({'status': 'progress', 'type': 'error', 'race_id': race['id']}) + '\n'

            # 3. 最終集計
            if all_results:
                analysis = calculate_analysis_summary(all_results)
                yield json.dumps({'status': 'success', 'results': all_results, 'analysis': analysis}) + '\n'
            else:
                yield json.dumps({'status': 'empty', 'message': '有効なレース結果が得られませんでした'}) + '\n'

        except Exception as e:
            import traceback
            yield json.dumps({'status': 'error', 'message': str(e)}) + '\n'
            print(f"Streaming error: {e}\n{traceback.format_exc()}")

    return Response(stream_with_context(generate()), mimetype='application/x-ndjson')

def process_single_race_internal(race, date_str, r_scraper, scraper, PREDICTION_CACHE, CACHE_LOCK, get_cached_prediction):
    """1レース分の的中判定を行う内部関数"""
    import re
    from itertools import combinations
    import datetime

    race_id = race['id']
    r_num = race.get('race_num', '?')
    venue = race.get('venue', '?')
    
    try:
        # 予測取得 (キャッシュ利用)
        prediction = get_cached_prediction(race_id)
        if not prediction: return None
        
        # 実結果取得 (SQLiteキャッシュ対応)
        actual = db.get_cached_result(race_id)
        if not actual:
            actual = r_scraper.scrape_result(race_id)
            if actual and actual.get('results'):
                db.save_result(race_id, date_str, actual)
        
        if not actual or not actual.get('results'): return None
        
        # 1-5着の馬番号 (従来の3着から5着に拡張)
        sorted_actual = sorted(actual['results'], key=lambda x: x.get('rank', 99))
        top5_nums = [str(r.get('number')) for r in sorted_actual[:5]]
        top3_nums = top5_nums[:3]
        top2_nums = top5_nums[:2]
        n_horses = len(actual['results'])
        
        # 軸(3) + ヒモ(5) の計8頭を取得
        axis_nums = [str(h['number']) for h in prediction.get('recommendations', [])[:3]]
        himo_all = [str(h['number']) for h in prediction.get('himo_horses', [])[:5]]
        himo_top3 = himo_all[:3]
        keshi_nums = [str(h['number']) for h in prediction.get('discouraged', [])[:3]]
        target_8_nums = axis_nums + himo_all
        
        # 的中判定ロジック
        axis_top1 = axis_nums[:1]
        axis_top2 = axis_nums[:2]
        axis_rest_from1 = axis_nums[1:]
        axis_rest_from2 = axis_nums[2:]
        top2_set = set(top2_nums)
        top3_set = set(top3_nums)

        def check_umaren(jiku_list, aite_list):
            pairs = set()
            for j in jiku_list:
                for a in aite_list:
                    if j != a: pairs.add(frozenset([j, a]))
                for j2 in jiku_list:
                    if j != j2: pairs.add(frozenset([j, j2]))
            target_pair = frozenset(top2_nums) if len(top2_nums) >= 2 else frozenset()
            return target_pair in pairs if target_pair else False
        
        def check_umaren_single_axis(jiku, nagashi_list):
            pairs = set()
            for a in nagashi_list:
                if jiku != a: pairs.add(frozenset([jiku, a]))
            target_pair = frozenset(top2_nums) if len(top2_nums) >= 2 else frozenset()
            return target_pair in pairs if target_pair else False

        hit_u5 = check_umaren_single_axis(axis_top1[0], axis_rest_from1 + himo_top3) if axis_top1 else False
        hit_u7 = check_umaren_single_axis(axis_top1[0], axis_rest_from1 + himo_all) if axis_top1 else False
        hit_u9 = check_umaren(axis_top2, axis_rest_from2 + himo_top3) if len(axis_top2) >= 2 else False
        hit_u13 = check_umaren(axis_top2, axis_rest_from2 + himo_all) if len(axis_top2) >= 2 else False
        hit_u18 = check_umaren(axis_nums, himo_all) if len(axis_nums) >= 2 else False

        def check_sanrenpuku_1axis(jiku, aite_list):
            combos = set()
            for c in combinations(aite_list, 2):
                combo = frozenset([jiku] + list(c))
                if len(combo) == 3: combos.add(combo)
            target = frozenset(top3_nums) if len(top3_nums) >= 3 else frozenset()
            return target in combos if target else False

        hit_t10 = check_sanrenpuku_1axis(axis_top1[0], axis_rest_from1 + himo_top3) if axis_top1 else False
        hit_t21 = check_sanrenpuku_1axis(axis_top1[0], axis_rest_from1 + himo_all) if axis_top1 else False

        # 三連複 16点(2軸): 軸1位と軸2位それぞれの1頭軸流し(相手5頭)
        # 軸1位流し(相手: 軸2, 軸3, ヒモ1, ヒモ2, ヒモ3) -> 5C2 = 10点
        hit_t16m_1 = check_sanrenpuku_1axis(axis_nums[0], axis_nums[1:3] + himo_top3) if len(axis_nums) >= 1 else False
        # 軸2位流し(相手: 軸3, ヒモ1, ヒモ2, ヒモ3) -> 4C2 = 6点
        hit_t16m_2 = check_sanrenpuku_1axis(axis_nums[1], axis_nums[2:3] + himo_top3) if len(axis_nums) >= 2 else False
        hit_t16m = hit_t16m_1 or hit_t16m_2
        
        # 三連複 16点(標準): 軸1位から(軸2,3位+ヒモ1-4位)への15点 + 軸2,3位のワイド的な抑え、または 軸1,2,3位のボックス(1点) + 軸1位流し(15点)
        hit_t16 = check_sanrenpuku_1axis(axis_top1[0], axis_rest_from1 + himo_all[:4]) or (all(n in axis_nums for n in top3_nums) if len(top3_nums)>=3 else False)
        
        is_all3_in_8 = all(n in target_8_nums for n in top3_nums) if len(top3_nums) >= 3 else False
        axis_hits_in_top3 = sum(1 for n in top3_nums if n in axis_nums)
        hit_t46 = is_all3_in_8 and (axis_hits_in_top3 >= 1)

        axis_any_top3 = any(n in top3_set for n in axis_nums)
        all8_cover_top3 = all(n in set(target_8_nums) for n in top3_nums) if len(top3_nums) >= 3 else False
        keshi_in_top3 = any(n in top3_set for n in keshi_nums)

        pay_trio = 0
        pay_quinella = 0
        for p in actual.get('payouts', []):
            p_label = p.get('type', '')
            val_str = re.sub(r'[^\d]', '', p.get('payout', '0'))
            val = int(val_str) if val_str else 0
            if ('三連' in p_label or '3連' in p_label) and '複' in p_label: pay_trio = val
            if '馬連' in p_label: pay_quinella = val
        
        # 常に最新の自信度をマージ
        res_data = {
            'date': date_str, 'race_num': r_num, 'venue': venue, 'id': race_id,
            'name': race.get('name'), 'surface': race.get('surface'), 'distance': race.get('distance'),
            'time': race.get('time'), 'class': race.get('race_class'), 'weight_cond': race.get('weight_cond'),
            'confidence': prediction.get('confidence', 0), # 自信度を追加
            'n_horses': n_horses, 'top3': top3_nums, 'top5': top5_nums, 'selection': target_8_nums,
            'hit_u5': hit_u5, 'hit_u7': hit_u7, 'hit_u9': hit_u9, 'hit_u13': hit_u13, 'hit_u18': hit_u18,
            'hit_t10': hit_t10, 'hit_t21': hit_t21, 'hit_t16m': hit_t16m, 'hit_t16': hit_t16, 'hit_t46': hit_t46,
            'axis_any_top3': axis_any_top3, 'all8_cover_top3': all8_cover_top3, 'keshi_in_top3': keshi_in_top3,
            'pay_trio': pay_trio, 'pay_quinella': pay_quinella
        }
        return res_data
    except Exception as e:
        print(f"Error processing {race_id}: {e}")
        return None

# 既存の get_verification_for_date を逐次処理に対応できるようにリファクタリング
def get_verification_for_date(date_str, filters=None):
    """指定日の検証データを取得する内部共通関数（フィルタ対応）"""
    from result_scraper import ResultScraper
    import concurrent.futures
    import re
    
    # 1. その日の全レースIDを取得
    res = scraper.get_races_by_date(date_str, skip_scoring=True)
    races = res.get('races', [])
    if not races:
        return None, None
        
    # フィルタ適用
    if filters:
        filtered_races = []
        for r in races:
            is_match = True
            r_num = int(r.get('race_num', 0))
            h_count = int(r.get('horse_count', 0))
            r_class = r.get('race_class', '')
            r_name = r.get('name', '')
            r_surf = r.get('surface', '')
            
            # 検索ワード (レース名 or クラス名)
            if filters.get('keyword'):
                kw = filters['keyword']
                if kw not in r_name and kw not in r_class: is_match = False
            
            # 競馬場 (複数選択対応)
            venues_str = filters.get('venues')
            if venues_str:
                venue_list = venues_str.split(',')
                if r.get('venue') not in venue_list: is_match = False
            
            # 馬場
            f_surf = filters.get('surface')
            if f_surf == 'turf' and '芝' not in r_surf: is_match = False
            elif f_surf == 'dirt' and 'ダート' not in r_surf: is_match = False
            elif f_surf == 'jump' and '障' not in r_surf: is_match = False
            
            # 条件
            if filters.get('shinba') and '新馬' not in r_class: is_match = False
            if filters.get('handicap') and not r.get('is_handicap'): is_match = False
            if filters.get('grade') and not any(g in r_class for g in ['G1', 'G2', 'G3', '重賞']): is_match = False
            if filters.get('mare') and not r.get('is_mare'): is_match = False
            
            # レース番号 (複数選択対応)
            race_nums_str = filters.get('race_nums')
            if race_nums_str:
                race_num_list = [int(x) for x in race_nums_str.split(',') if x.isdigit()]
                if r_num not in race_num_list: is_match = False
            
            # 頭数
            f_horses = filters.get('horses')
            if f_horses == 'lt10' and h_count > 10: is_match = False
            elif f_horses == '12' and h_count != 12: is_match = False
            elif f_horses == '12-14' and not (12 <= h_count <= 14): is_match = False
            elif f_horses == 'gt15' and h_count < 15: is_match = False
            
            if is_match:
                filtered_races.append(r)
        races = filtered_races

    if not races:
        return [], {
            'venue': {}, 'surface': {}, 'horse_count': {},
            'summary': {'t16': {'inv':0,'ret':0,'hits':0,'roi':0,'hit_rate':0,'name':'三連複 16点'},
                        't46': {'inv':0,'ret':0,'hits':0,'roi':0,'hit_rate':0,'name':'三連複 46点'},
                        'u18': {'inv':0,'ret':0,'hits':0,'roi':0,'hit_rate':0,'name':'馬連 18点'}}
        }

    r_scraper = ResultScraper()
    results = []
    
    def process_single_race(race):
        race_id = race['id']
        r_num = race.get('race_num', '?')
        venue = race.get('venue', '?')
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]   --- {venue}{r_num}R ({race_id}) no kaiseki kaishi...")
        try:
            # 予想取得 (キャッシュ利用)
            prediction = get_cached_prediction(race_id)
            if not prediction: 
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]   [NG] {venue}{r_num}R: yosou data shutoku shippai")
                return None
            
            # 実結果取得
            actual = r_scraper.scrape_result(race_id)
            if not actual or not actual.get('results'):
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]   [NG] {venue}{r_num}R: jitsu kekka shutoku shippai")
                return None
            
            # 1-3着の馬番号
            sorted_actual = sorted(actual['results'], key=lambda x: x.get('rank', 99))
            top3_nums = [str(r.get('number')) for r in sorted_actual[:3]]
            top2_nums = top3_nums[:2]
            n_horses = len(actual['results'])
            
            # 軸(3) + ヒモ(5) の計8頭を取得
            axis_nums = [str(h['number']) for h in prediction.get('recommendations', [])[:3]]
            himo_all = [str(h['number']) for h in prediction.get('himo_horses', [])[:5]]
            himo_top3 = himo_all[:3]  # ヒモ上位3頭
            keshi_nums = [str(h['number']) for h in prediction.get('discouraged', [])[:3]]
            target_8_nums = axis_nums + himo_all
            
            # 基本パーツ
            axis_top1 = axis_nums[:1]  # 軸馬TOP1
            axis_top2 = axis_nums[:2]  # 軸馬TOP2
            axis_rest_from1 = axis_nums[1:]  # TOP1以外の軸
            axis_rest_from2 = axis_nums[2:]  # TOP2以外の軸

            # 1-2着および1-3着の集合
            top2_set = set(top2_nums)
            top3_set = set(top3_nums)

            # --- 馬連の的中判定ロジック（1-2着が対象） ---
            def check_umaren(jiku_list, aite_list):
                """軸-相手流し＋軸同士の馬連的中判定"""
                # 軸同士の組み合わせも含む
                pairs = set()
                for j in jiku_list:
                    for a in aite_list:
                        if j != a:
                            pairs.add(frozenset([j, a]))
                    # 軸同士
                    for j2 in jiku_list:
                        if j != j2:
                            pairs.add(frozenset([j, j2]))
                target_pair = frozenset(top2_nums) if len(top2_nums) >= 2 else frozenset()
                return target_pair in pairs if target_pair else False
            
            def check_umaren_single_axis(jiku, nagashi_list):
                """単一軸の馬連流し（軸同士なし）"""
                pairs = set()
                for a in nagashi_list:
                    if jiku != a:
                        pairs.add(frozenset([jiku, a]))
                target_pair = frozenset(top2_nums) if len(top2_nums) >= 2 else frozenset()
                return target_pair in pairs if target_pair else False

            # ① 馬連5点: 軸1頭(TOP1)→残軸2+ヒモ3流し
            hit_u5 = check_umaren_single_axis(axis_top1[0], axis_rest_from1 + himo_top3) if axis_top1 else False
            # ② 馬連7点: 軸1頭(TOP1)→残軸2+ヒモ5流し
            hit_u7 = check_umaren_single_axis(axis_top1[0], axis_rest_from1 + himo_all) if axis_top1 else False
            # ③ 馬連9点: 軸2頭(TOP2)→残軸1+ヒモ3流し+軸同士
            hit_u9 = check_umaren(axis_top2, axis_rest_from2 + himo_top3) if len(axis_top2) >= 2 else False
            # ④ 馬連13点: 軸2頭(TOP2)→残軸1+ヒモ5流し+軸同士
            hit_u13 = check_umaren(axis_top2, axis_rest_from2 + himo_all) if len(axis_top2) >= 2 else False
            # ⑤ 馬連18点: 既存（軸3→ヒモ5+軸同士）
            hit_u18 = check_umaren(axis_nums, himo_all) if len(axis_nums) >= 2 else False

            # --- 三連複の的中判定ロジック（1-3着が対象） ---
            from itertools import combinations
            def check_sanrenpuku_1axis(jiku, aite_list):
                """1頭軸三連複流し: 軸を含む3頭の組み合わせ"""
                combos = set()
                for c in combinations(aite_list, 2):
                    combo = frozenset([jiku] + list(c))
                    if len(combo) == 3:
                        combos.add(combo)
                target = frozenset(top3_nums) if len(top3_nums) >= 3 else frozenset()
                return target in combos if target else False

            # ⑥ 三連複10点: 軸1頭(TOP1)→残軸2+ヒモ3
            hit_t10 = check_sanrenpuku_1axis(axis_top1[0], axis_rest_from1 + himo_top3) if axis_top1 else False
            # ⑦ 三連複21点: 軸1頭(TOP1)→残軸2+ヒモ5
            hit_t21 = check_sanrenpuku_1axis(axis_top1[0], axis_rest_from1 + himo_all) if axis_top1 else False
            # ⑧ 三連複 16点(2軸): 軸1位と軸2位それぞれの1頭軸流し(相手5頭)
            # 軸1位流し(相手: 軸2, 軸3, ヒモ1, ヒモ2, ヒモ3) -> 5C2 = 10点
            hit_t16m_1 = check_sanrenpuku_1axis(axis_nums[0], axis_nums[1:3] + himo_top3) if len(axis_nums) >= 1 else False
            # 軸2位流し(相手: 軸3, ヒモ1, ヒモ2, ヒモ3) -> 4C2 = 6点
            hit_t16m_2 = check_sanrenpuku_1axis(axis_nums[1], axis_nums[2:3] + himo_top3) if len(axis_nums) >= 2 else False
            hit_t16m = hit_t16m_1 or hit_t16m_2
            # ⑨ 三連複16点: 既存（軸3頭, 1-2頭が軸, 残りヒモ5流し）
            is_all3_in_8 = all(n in target_8_nums for n in top3_nums) if len(top3_nums) >= 3 else False
            axis_hits_in_top3 = sum(1 for n in top3_nums if n in axis_nums)
            hit_t16 = is_all3_in_8 and (axis_hits_in_top3 >= 2)
            # ⑩ 三連複46点: 既存（軸3→ヒモ5マルチ）
            hit_t46 = is_all3_in_8 and (axis_hits_in_top3 >= 1)

            # --- 追加統計 ---
            axis_any_top3 = any(n in top3_set for n in axis_nums)
            all8_cover_top3 = all(n in set(target_8_nums) for n in top3_nums) if len(top3_nums) >= 3 else False
            keshi_in_top3 = any(n in top3_set for n in keshi_nums)

            pay_trio = 0
            pay_quinella = 0
            # 配当の解析
            for p in actual.get('payouts', []):
                p_label = p.get('type', '')
                val_str = re.sub(r'[^\d]', '', p.get('payout', '0'))
                val = int(val_str) if val_str else 0
                if ('三連' in p_label or '3連' in p_label) and '複' in p_label:
                    pay_trio = val
                if '馬連' in p_label:
                    pay_quinella = val
            
            t16_txt = 'HIT' if hit_t16 else 'miss'
            u18_txt = 'HIT' if hit_u18 else 'miss'
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]   [OK] {venue}{r_num}R: hantei kanryou (trio16:{t16_txt}, umaren18:{u18_txt})")
            return {
                'date': date_str,
                'race_num': r_num,
                'venue': venue,
                'name': race.get('name'),
                'surface': race.get('surface'),
                'distance': race.get('distance'),
                'n_horses': n_horses,
                'top3': top3_nums,
                'top5': top5_nums, # 5着まで返す
                'selection': target_8_nums,
                'class': race.get('race_class'),
                # 馬連
                'hit_u5': hit_u5, 'hit_u7': hit_u7, 'hit_u9': hit_u9,
                'hit_u13': hit_u13, 'hit_u18': hit_u18,
                # 三連複
                'hit_t10': hit_t10, 'hit_t21': hit_t21, 'hit_t16m': hit_t16m,
                'hit_t16': hit_t16, 'hit_t46': hit_t46,
                # 追加統計
                'axis_any_top3': axis_any_top3,
                'all8_cover_top3': all8_cover_top3,
                'keshi_in_top3': keshi_in_top3,
                # 配当
                'pay_trio': pay_trio,
                'pay_quinella': pay_quinella
            }
        except Exception as e:
            print(f"Error processing {race_id}: {e}")
            return None

    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {date_str} kenshou kaishi (taishou:{len(races)} race)...")
    
    # 並行処理で一件ずつ計算 (リファクタリング後の内部関数を利用)
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        fs = [executor.submit(process_single_race_internal, r, date_str, r_scraper, scraper, PREDICTION_CACHE, CACHE_LOCK, get_cached_prediction) for r in races]
        for f in concurrent.futures.as_completed(fs):
            item = f.result()
            if item: results.append(item)
            
    results.sort(key=lambda x: int(x['race_num']) if str(x['race_num']).isdigit() else 0)
    
    # 的中分析
    venue_hits = {}
    surface_hits = {}
    horse_count_hits = {} 
    
    # 全10戦略の定義
    STRATEGIES = [
        {'key': 'u5',   'name': '① 馬連 5点',  'points': 5,  'pay_field': 'pay_quinella', 'desc': '軸馬1位から(軸2,3位+ヒモ1~3位)へ流し'},
        {'key': 'u7',   'name': '② 馬連 7点',  'points': 7,  'pay_field': 'pay_quinella', 'desc': '軸馬1位から(軸2,3位+ヒモ1~5位)へ流し'},
        {'key': 'u9',   'name': '③ 馬連 9点',  'points': 9,  'pay_field': 'pay_quinella', 'desc': '軸馬1,2位から(軸3位+ヒモ1~3位)へ流し(重複除外)'},
        {'key': 'u13',  'name': '④ 馬連 13点', 'points': 13, 'pay_field': 'pay_quinella', 'desc': '軸馬1,2位から(軸3位+ヒモ1~5位)へ流し(重複除外)'},
        {'key': 'u18',  'name': '⑤ 馬連 18点', 'points': 18, 'pay_field': 'pay_quinella', 'desc': '軸馬1~3位からヒモ1~5位への流し＋軸馬BOX'},
        {'key': 't10',  'name': '⑥ 三連複 10点', 'points': 10, 'pay_field': 'pay_trio', 'desc': '軸馬1位の1頭軸、相手(軸2,3位+ヒモ1~3位)の5頭流し'},
        {'key': 't21',  'name': '⑦ 三連複 21点', 'points': 21, 'pay_field': 'pay_trio', 'desc': '軸馬1位の1頭軸、相手(軸2,3位+ヒモ1~5位)の7頭流し'},
        {'key': 't16m', 'name': '⑧ 三連複 16点(2軸)', 'points': 16, 'pay_field': 'pay_trio', 'desc': '軸馬1位・2位それぞれの1頭軸から相手5頭への流し(10点+6点)'},
        {'key': 't16',  'name': '⑨ 三連複 16点', 'points': 16, 'pay_field': 'pay_trio', 'desc': '軸馬1~3位の中から2頭以上を含めたフォーメーション等(15点+BOX1点)'},
        {'key': 't46',  'name': '⑩ 三連複 46点', 'points': 46, 'pay_field': 'pay_trio', 'desc': '軸馬いずれか1頭以上を含む8頭BOX相当'},
    ]

    summary = {}
    for st in STRATEGIES:
        summary[st['key']] = {
            'inv': 0, 'ret': 0, 'hits': 0, 
            'min_pay': 999999, 'max_pay': 0, 
            'name': st['name'], 'points': st['points']
        }
    
    extra_stats = {'axis_any_top3': 0, 'all8_cover_top3': 0, 'keshi_in_top3': 0, 'total': 0}

    def init_cat_entry():
        entry = {'total': 0}
        for st in STRATEGIES:
            entry[st['key']] = 0
            entry[st['key'] + '_ret'] = 0
            entry[st['key'] + '_inv'] = 0
        return entry

    for r in results:
        v = r['venue']
        s = r['surface']
        nh = r['n_horses']
        if nh <= 11: h_range = "11頭以下"
        elif nh <= 14: h_range = "12-14頭"
        else: h_range = "15-18頭"

        for cat_map, key in [(venue_hits, v), (surface_hits, s), (horse_count_hits, h_range)]:
            if key not in cat_map: 
                cat_map[key] = init_cat_entry()
            cat_map[key]['total'] += 1
            for st in STRATEGIES:
                cat_map[key][st['key'] + '_inv'] += st['points'] * 100
        
        for st in STRATEGIES:
            summary[st['key']]['inv'] += st['points'] * 100
        
        extra_stats['total'] += 1
        if r.get('axis_any_top3'): extra_stats['axis_any_top3'] += 1
        if r.get('all8_cover_top3'): extra_stats['all8_cover_top3'] += 1
        if r.get('keshi_in_top3'): extra_stats['keshi_in_top3'] += 1

        for st in STRATEGIES:
            hit_key = 'hit_' + st['key']
            if r.get(hit_key):
                pay = r.get(st['pay_field'], 0)
                summary[st['key']]['ret'] += pay
                summary[st['key']]['hits'] += 1
                if pay > 0:
                    summary[st['key']]['min_pay'] = min(summary[st['key']]['min_pay'], pay)
                    summary[st['key']]['max_pay'] = max(summary[st['key']]['max_pay'], pay)
                for cat_map, key in [(venue_hits, v), (surface_hits, s), (horse_count_hits, h_range)]:
                    cat_map[key][st['key']] += 1
                    cat_map[key][st['key'] + '_ret'] += pay

    total_r = len(results)
    for k in summary:
        item = summary[k]
        item['hit_rate'] = round(item['hits'] / total_r * 100, 1) if total_r > 0 else 0
        item['roi'] = round(item['ret'] / item['inv'] * 100, 1) if item['inv'] > 0 else 0
        if item['min_pay'] == 999999: item['min_pay'] = 0

    if extra_stats['total'] > 0:
        t = extra_stats['total']
        extra_stats['axis_any_top3_rate'] = round(extra_stats['axis_any_top3'] / t * 100, 1)
        extra_stats['all8_cover_top3_rate'] = round(extra_stats['all8_cover_top3'] / t * 100, 1)
        extra_stats['keshi_in_top3_rate'] = round(extra_stats['keshi_in_top3'] / t * 100, 1)

    analysis = {
        'venue': venue_hits,
        'surface': surface_hits,
        'horse_count': horse_count_hits,
        'summary': summary,
        'extra_stats': extra_stats,
        'strategies_def': [{'key': st['key'], 'name': st['name'], 'points': st['points'], 'pay_field': st['pay_field'], 'desc': st['desc']} for st in STRATEGIES]
    }
    return results, analysis

@app.route('/api/verify_period_predictions')
def verify_period_predictions():
    """指定期間の全レースについて、的中検証を行い逐次結果を返す (SSE/NDJSON)"""
    from flask import Response, stream_with_context
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({'error': 'Start and end dates are required'}), 400
        
    filters = {
        'keyword': request.args.get('keyword'),
        'surface': request.args.get('surface'),
        'venues': request.args.get('venues'),
        'race_nums': request.args.get('race_nums'),
        'horses': request.args.get('horses'),
        'shinba': request.args.get('shinba') == '1',
        'handicap': request.args.get('handicap') == '1',
        'grade': request.args.get('grade') == '1',
        'mare': request.args.get('mare') == '1'
    }

    force = request.args.get('force') == '1'

    def generate():
        try:
            from datetime import datetime, timedelta
            s_dt = datetime.strptime(start_date, '%Y%m%d')
            e_dt = datetime.strptime(end_date, '%Y%m%d')
            
            target_races = []
            curr = s_dt
            while curr <= e_dt:
                d_str = curr.strftime('%Y%m%d')
                if force:
                    with CACHE_LOCK:
                        keys_to_del = [k for k in PREDICTION_CACHE.keys() if k.startswith(d_str)]
                        for k in keys_to_del: del PREDICTION_CACHE[k]

                res = scraper.get_races_by_date(d_str, skip_scoring=True)
                day_races = res.get('races', [])
                for r in day_races:
                    is_match = True
                    try:
                        r_num_str = r.get('race_num', '0')
                        r_num = int(r_num_str) if str(r_num_str).isdigit() else 0
                        h_count_str = r.get('horse_count', '0')
                        h_count = int(h_count_str) if str(h_count_str).isdigit() else 0
                        r_class = r.get('race_class', '')
                        r_name = r.get('name', '')
                        r_surf = r.get('surface', '')
                        
                        if filters.get('keyword'):
                            kw = filters['keyword']
                            if kw not in r_name and kw not in r_class: is_match = False
                        
                        venues_str = filters.get('venues')
                        if venues_str:
                            venue_list = venues_str.split(',')
                            if r.get('venue') not in venue_list: is_match = False
                        
                        f_surf = filters.get('surface')
                        if f_surf == 'turf' and '芝' not in r_surf: is_match = False
                        elif f_surf == 'dirt' and 'ダート' not in r_surf: is_match = False
                        elif f_surf == 'jump' and '障' not in r_surf: is_match = False
                        
                        if filters.get('shinba') and '新馬' not in r_class: is_match = False
                        if filters.get('handicap') and not r.get('is_handicap'): is_match = False
                        if filters.get('grade') and not any(g in r_class for g in ['G1', 'G2', 'G3', '重賞']): is_match = False
                        if filters.get('mare') and not r.get('is_mare'): is_match = False
                        
                        race_nums_str = filters.get('race_nums')
                        if race_nums_str:
                            race_num_list = [int(x) for x in race_nums_str.split(',') if x.isdigit()]
                            if r_num not in race_num_list: is_match = False
                        
                        f_horses = filters.get('horses')
                        if f_horses == 'lt10' and h_count > 10: is_match = False
                        elif f_horses == '12' and h_count != 12: is_match = False
                        elif f_horses == '12-14' and not (12 <= h_count <= 14): is_match = False
                        elif f_horses == 'gt15' and h_count < 15: is_match = False
                    except: is_match = False
                    
                    if is_match: target_races.append((r, d_str))
                curr += timedelta(days=1)

            yield json.dumps({'status': 'progress', 'type': 'start', 'total': len(target_races)}) + '\n'

            from result_scraper import ResultScraper
            r_scraper = ResultScraper()
            all_results = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                future_to_race = {executor.submit(process_single_race_internal, race, d_str, r_scraper, scraper, PREDICTION_CACHE, CACHE_LOCK, get_cached_prediction): (race, d_str) for race, d_str in target_races}
                
                for future in concurrent.futures.as_completed(future_to_race):
                    res_item = future.result()
                    if res_item:
                        all_results.append(res_item)
                        yield json.dumps({'status': 'progress', 'type': 'race', 'data': res_item}) + '\n'
                    else:
                        race, d_str = future_to_race[future]
                        yield json.dumps({'status': 'progress', 'type': 'error', 'race_id': race['id']}) + '\n'

            if all_results:
                analysis = calculate_analysis_summary(all_results)
                yield json.dumps({'status': 'success', 'results': all_results, 'analysis': analysis}) + '\n'
            else:
                yield json.dumps({'status': 'empty', 'message': '有効なレース結果が得られませんでした'}) + '\n'

        except Exception as e:
            import traceback, datetime
            yield json.dumps({'status': 'error', 'message': str(e)}) + '\n'
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Stream period error: {e}\n{traceback.format_exc()}")

    return Response(stream_with_context(generate()), mimetype='application/x-ndjson')

def calculate_analysis_summary(results):
    """結果リストから集計サマリを作成する共通関数（10戦略対応）"""
    STRATEGIES = [
        {'key': 'u5',   'name': '① 馬連 5点',  'points': 5,  'pay_field': 'pay_quinella', 'desc': '軸馬1位から(軸2,3位+ヒモ1~3位)へ流し'},
        {'key': 'u7',   'name': '② 馬連 7点',  'points': 7,  'pay_field': 'pay_quinella', 'desc': '軸馬1位から(軸2,3位+ヒモ1~5位)へ流し'},
        {'key': 'u9',   'name': '③ 馬連 9点',  'points': 9,  'pay_field': 'pay_quinella', 'desc': '軸馬1,2位から(軸3位+ヒモ1~3位)へ流し(重複除外)'},
        {'key': 'u13',  'name': '④ 馬連 13点', 'points': 13, 'pay_field': 'pay_quinella', 'desc': '軸馬1,2位から(軸3位+ヒモ1~5位)へ流し(重複除外)'},
        {'key': 'u18',  'name': '⑤ 馬連 18点', 'points': 18, 'pay_field': 'pay_quinella', 'desc': '軸馬1~3位からヒモ1~5位への流し＋軸馬BOX'},
        {'key': 't10',  'name': '⑥ 三連複 10点', 'points': 10, 'pay_field': 'pay_trio', 'desc': '軸馬1位の1頭軸、相手(軸2,3位+ヒモ1~3位)の5頭流し'},
        {'key': 't21',  'name': '⑦ 三連複 21点', 'points': 21, 'pay_field': 'pay_trio', 'desc': '軸馬1位の1頭軸、相手(軸2,3位+ヒモ1~5位)の7頭流し'},
        {'key': 't16m', 'name': '⑧ 三連複 16点(2軸)', 'points': 16, 'pay_field': 'pay_trio', 'desc': '軸馬1位・2位それぞれの1頭軸から相手5頭への流し(10点+6点)'},
        {'key': 't16',  'name': '⑨ 三連複 16点', 'points': 16, 'pay_field': 'pay_trio', 'desc': '軸馬1~3位の中から2頭以上を含めたフォーメーション等(15点+BOX1点)'},
        {'key': 't46',  'name': '⑩ 三連複 46点', 'points': 46, 'pay_field': 'pay_trio', 'desc': '軸馬いずれか1頭以上を含む8頭BOX相当'},
    ]

    venue_hits = {}
    surface_hits = {}
    horse_count_hits = {}
    
    summary = {}
    for st in STRATEGIES:
        summary[st['key']] = {
            'inv': 0, 'ret': 0, 'hits': 0,
            'min_pay': 999999, 'max_pay': 0,
            'name': st['name'], 'points': st['points']
        }

    extra_stats = {'axis_any_top3': 0, 'all8_cover_top3': 0, 'keshi_in_top3': 0, 'total': 0}

    def init_cat_entry():
        entry = {'total': 0}
        for st in STRATEGIES:
            entry[st['key']] = 0
            entry[st['key'] + '_ret'] = 0
            entry[st['key'] + '_inv'] = 0
        return entry

    for r in results:
        v = r['venue']
        s = r['surface']
        nh = r['n_horses']
        if nh <= 11: h_range = "11頭以下"
        elif nh <= 14: h_range = "12-14頭"
        else: h_range = "15-18頭"

        for cat_map, key in [(venue_hits, v), (surface_hits, s), (horse_count_hits, h_range)]:
            if key not in cat_map:
                cat_map[key] = init_cat_entry()
            cat_map[key]['total'] += 1
            for st in STRATEGIES:
                cat_map[key][st['key'] + '_inv'] += st['points'] * 100

        for st in STRATEGIES:
            summary[st['key']]['inv'] += st['points'] * 100

        extra_stats['total'] += 1
        if r.get('axis_any_top3'): extra_stats['axis_any_top3'] += 1
        if r.get('all8_cover_top3'): extra_stats['all8_cover_top3'] += 1
        if r.get('keshi_in_top3'): extra_stats['keshi_in_top3'] += 1

        for st in STRATEGIES:
            hit_key = 'hit_' + st['key']
            if r.get(hit_key):
                pay = r.get(st['pay_field'], 0)
                summary[st['key']]['ret'] += pay
                summary[st['key']]['hits'] += 1
                if pay > 0:
                    summary[st['key']]['min_pay'] = min(summary[st['key']]['min_pay'], pay)
                    summary[st['key']]['max_pay'] = max(summary[st['key']]['max_pay'], pay)
                for cat_map, key in [(venue_hits, v), (surface_hits, s), (horse_count_hits, h_range)]:
                    cat_map[key][st['key']] += 1
                    cat_map[key][st['key'] + '_ret'] += pay

    total_r = len(results)
    for k in summary:
        item = summary[k]
        item['hit_rate'] = round(item['hits'] / total_r * 100, 1) if total_r > 0 else 0
        item['roi'] = round(item['ret'] / item['inv'] * 100, 1) if item['inv'] > 0 else 0
        if item['min_pay'] == 999999: item['min_pay'] = 0

    if extra_stats['total'] > 0:
        t = extra_stats['total']
        extra_stats['axis_any_top3_rate'] = round(extra_stats['axis_any_top3'] / t * 100, 1)
        extra_stats['all8_cover_top3_rate'] = round(extra_stats['all8_cover_top3'] / t * 100, 1)
        extra_stats['keshi_in_top3_rate'] = round(extra_stats['keshi_in_top3'] / t * 100, 1)

    return {
        'venue': venue_hits,
        'surface': surface_hits,
        'horse_count': horse_count_hits,
        'summary': summary,
        'extra_stats': extra_stats,
        'strategies_def': [{'key': st['key'], 'name': st['name'], 'points': st['points'], 'pay_field': st['pay_field'], 'desc': st['desc']} for st in STRATEGIES]
    }


@app.route('/api/show_race_result')
def show_race_result():
    """Step 1: レース結果（1-5着＋配当）を取得し、予想との乖離を返す（まだAIには保存しない）"""
    race_id = request.args.get('race_id')
    if not race_id:
        return jsonify({'status': 'error', 'message': 'Race ID is required'}), 400
    
    try:
        from result_scraper import ResultScraper
        from error_analyzer import ErrorAnalyzer
        import pandas as pd

        # 1. 結果をスクレイピング
        r_scraper = ResultScraper()
        result_data = r_scraper.scrape_result(race_id)
        if not result_data or not result_data.get('results'):
             return jsonify({'status': 'error', 'message': '結果が見つかりません（まだ終了していない可能性があります）'}), 400

        # 2. 上位5着までの結果を抽出
        sorted_results = sorted(result_data['results'], key=lambda x: x.get('rank', 99))
        top5 = []
        for r in sorted_results[:5]:
            top5.append({
                'rank': r.get('rank', '-'),
                'number': r.get('number', '-'),
                'horse_name': r.get('horse_name', '-'),
                'jockey_name': r.get('jockey_name', '-'),
                'pop': r.get('pop', '-'),
                'odds': r.get('odds', 0),
                'time_str': r.get('time_str', '-'),
            })

        # 3. 配当情報の取得（スクレイピング結果から取得）
        payouts = result_data.get('payouts', [])
        
        # フォールバック（万が一配当が取得できなかった場合のみ、従来通り単勝を推定）
        if not payouts and top5:
            win_odds = top5[0].get('odds', 0)
            if win_odds > 0:
                payouts.append({
                    'type': '単勝', 
                    'number': top5[0]['number'],
                    'horse': top5[0]['horse_name'], 
                    'payout': f"{int(win_odds * 100)}円"
                })
        else:
            # スクレイピング結果の形式をフロントエンド用に調整
            for p in payouts:
                # 'number' フィールドが馬番号だけでなく馬名も含んでいる場合があるため、表示用に調整
                # (ResultScraperの実装に依存するが、安全のためそのまま渡すか微調整)
                pass

        # 4. 予想を生成して乖離分析 (キャッシュ利用)
        prediction = get_cached_prediction(race_id)
        gap_analysis = {'axis': [], 'himo': [], 'discouraged': []}
        discrepancies_data = []
        
        if prediction:
            # 軸馬の結果
            for rec in prediction.get('recommendations', []):
                actual = next((r for r in sorted_results if r.get('number') == rec['number']), None)
                actual_rank = actual['rank'] if actual else '不明'
                status = '◎的中' if actual and actual['rank'] <= 3 else '✕外れ'
                gap_analysis['axis'].append({
                    'number': rec['number'], 'name': rec['name'],
                    'predicted': f"軸馬{rec.get('mark', '')}",
                    'actual_rank': actual_rank, 'status': status
                })
            
            # ヒモ馬の結果
            for h in prediction.get('himo_horses', []):
                actual = next((r for r in sorted_results if r.get('number') == h['number']), None)
                actual_rank = actual['rank'] if actual else '不明'
                status = '◎的中' if actual and actual['rank'] <= 3 else '△惜しい' if actual and actual['rank'] <= 5 else '✕外れ'
                gap_analysis['himo'].append({
                    'number': h['number'], 'name': h['name'],
                    'predicted': 'ヒモ馬候補',
                    'actual_rank': actual_rank, 'status': status
                })
            
            # 消し馬の結果
            for d in prediction.get('discouraged', []):
                actual = next((r for r in sorted_results if r.get('number') == d['number']), None)
                actual_rank = actual['rank'] if actual else '不明'
                status = '★激走!' if actual and actual['rank'] <= 3 else '消し正解'
                gap_analysis['discouraged'].append({
                    'number': d['number'], 'name': d['name'],
                    'predicted': '消し馬',
                    'actual_rank': actual_rank, 'status': status
                })
            
            # 乖離分析データ（confirm_feedbackで使う）
            analyzer = ErrorAnalyzer()
            actual_results_df = pd.DataFrame(result_data['results'])
            discrepancies = analyzer.analyze_race_discrepancy(prediction, actual_results_df, prediction['race_info'])
            for d in discrepancies:
                discrepancies_data.append({
                    "type": str(d.get('type', 'unknown')),
                    "horse": str(d.get('horse', 'unknown')),
                    "message": str(d.get('message', '')),
                })

        return jsonify({
            'status': 'success',
            'top5': top5,
            'payouts': payouts,
            'gap_analysis': gap_analysis,
            'discrepancies': discrepancies_data,
            'race_info': result_data.get('race_info', {})
        })

    except Exception as e:
        import traceback, datetime
        err_msg = f"検証エラー: {str(e)}"
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Exception Error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'status': 'error', 'message': err_msg}), 500

@app.route('/api/confirm_feedback')
def confirm_feedback():
    """Step 2: 乖離データをAIに保存し、アルゴリズムに反映する"""
    race_id = request.args.get('race_id')
    if not race_id:
        return jsonify({'status': 'error', 'message': 'Race ID is required'}), 400
    
    try:
        from result_scraper import ResultScraper
        from error_analyzer import ErrorAnalyzer
        import pandas as pd

        # 結果を再取得してフィードバック蓄積
        r_scraper = ResultScraper()
        result_data = r_scraper.scrape_result(race_id)
        if not result_data or not result_data.get('results'):
             return jsonify({'status': 'error', 'message': '結果データが取得できません'}), 400

        # 予測データ取得 (キャッシュ立優先)
        prediction = get_cached_prediction(race_id)
        if not prediction:
            return jsonify({'status': 'error', 'message': '予想データの生成に失敗しました'}), 500

        analyzer = ErrorAnalyzer()
        actual_results_df = pd.DataFrame(result_data['results'])
        discrepancies = analyzer.analyze_race_discrepancy(prediction, actual_results_df, prediction['race_info'])

        # フィードバックを保存
        feedback_path = os.path.join(os.path.dirname(__file__), "feedback_data.json")
        feedback = {"patterns": [], "last_updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        
        if os.path.exists(feedback_path):
            with open(feedback_path, 'r', encoding='utf-8') as f:
                try:
                    loaded = json.load(f)
                    if isinstance(loaded, dict) and "patterns" in loaded:
                        feedback = loaded
                except: pass

        new_patterns = []
        race_info = prediction.get('race_info', {})
        horses_list = prediction.get('horses', [])
        
        for d in discrepancies:
            h_name = str(d.get('horse', 'unknown'))
            # 激走判定: 指数 10位以下（消し馬）が 3着以内
            is_underdog_upset = False
            for idx, h_item in enumerate(horses_list):
                if h_item.get('name') == h_name and idx >= 9:
                    # 実際の着順を確認
                    for _, row in actual_results_df.iterrows():
                        actual_rank_raw = row.get('rank', 99)
                        try:
                            actual_rank = int(actual_rank_raw)
                        except (ValueError, TypeError):
                            actual_rank = 99
                            
                        if str(row['horse_name']) == h_name and actual_rank <= 3:
                            is_underdog_upset = True
                            break
                    break
            
            if is_underdog_upset:
                new_patterns.append({
                    "pattern": "underdog_upset",
                    "horse_name": h_name,
                    "venue": race_info.get('venue'),
                    "distance": race_info.get('distance'),
                    "surface": race_info.get('surface'),
                    "message": f"【激走】指数下位ながら好走。{race_info.get('venue')} {race_info.get('distance')}m 注意。",
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
            else:
                new_patterns.append({
                    "type": str(d.get('type', 'unknown')),
                    "horse": h_name,
                    "message": str(d.get('message', '')),
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
            
        feedback["patterns"].extend(new_patterns)
        feedback["patterns"] = feedback["patterns"][-100:]  # 直近100件
        feedback["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(feedback_path, 'w', encoding='utf-8') as f:
            json.dump(feedback, f, ensure_ascii=False, indent=4)

        # キャッシュの削除: 次回予測時に最新のフィードバックを反映させるため
        PREDICTION_CACHE.pop(race_id, None)

        msg = f'✅ {len(new_patterns)}件の乖離パターンを新しいアルゴリズムに反映しました！'
        if not new_patterns:
            msg = '✅ 特筆すべき乖離はありませんでしたが、アルゴリズムは最新状態です。'

        return jsonify({
            'status': 'success', 
            'message': msg,
            'patterns_count': len(new_patterns)
        })

    except Exception as e:
        import traceback, datetime
        err_msg = f"検証エラー: {str(e)}"
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Exception Error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'status': 'error', 'message': err_msg}), 500


@app.route('/api/run_optimization')
def run_optimization():
    """最適化(Optimizer)を実行してパラメータを更新する"""
    try:
        from optimizer import Optimizer
        opt = Optimizer()
        # 蓄積されたDBデータからパラメータを調整
        success = opt.update_from_db()
        if success:
            # 全キャッシュのクリア: 最適化された新パラメータを全レースに反映させるため
            PREDICTION_CACHE.clear()
            
            params_path = os.path.join(os.path.dirname(__file__), "optimized_params.json")
            msg = 'AIパラメータを統計データに基づき最適化しました'
            last_updated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            if os.path.exists(params_path):
                with open(params_path, 'r', encoding='utf-8') as f:
                    try:
                        params = json.load(f)
                        last_updated = params.get('last_updated', last_updated)
                    except: pass
            
            return jsonify({
                'status': 'success',
                'message': msg,
                'last_updated': last_updated
            })
        else:
            return jsonify({
                'status': 'error', 
                'message': '最適化に必要なデータが蓄積されていません（件数不足）'
            }), 400
    except Exception as e:
        import traceback, datetime
        err_msg = f"検証エラー: {str(e)}"
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Exception Error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'status': 'error', 'message': err_msg}), 500


@app.route('/api/get_best_races')
def get_best_races():
    """指定日の全レースから「荒れやすい＆当たりやすい」おすすめ上位5レースを抽出する"""
    date_str = request.args.get('date')
    venue_filter = request.args.get('venue', '')  # 競馬場フィルタ（空なら全場）
    if not date_str:
        return jsonify({'error': 'Date is required'}), 400
    
    try:
        # ローカル会場リスト
        LOCAL_VENUES = ['福島', '小倉', '新潟', '函館', '札幌']
        
        # 1. その日の全レースIDを取得
        res = scraper.get_races_by_date(date_str, skip_scoring=True)
        races = res.get('races', [])
        if not races:
            return jsonify({'status': 'empty', 'message': '該当日のレースが見つかりません'})
        
        # 開催会場リストの収集（フロントエンド用：常に全会場分を返す）
        venues = sorted(list(set(r.get('venue', '').strip() for r in races if r.get('venue'))))
        if not venues:
            venues = sorted(list(set(r.get('name', '')[:2] for r in races if r.get('name'))))
        
        all_venues = venues # フィルタ前の全会場リストを保持
        
        # 競馬場フィルタ適用
        if venue_filter:
            races = [r for r in races if r.get('venue', '').strip() == venue_filter.strip()]
            if not races:
                return jsonify({'status': 'empty', 'message': f'{venue_filter}のレースは見つかりません', 'venues': all_venues})
            
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] おすすめレース抽出開始: {date_str} (計 {len(races)}レース, 会場: {venue_filter or '全場'})")
        
        analyzed_races = []
        
        # 2. 全レースを並行して分析
        def analyze_single(r):
            try:
                # 予想取得 (キャッシュ利用)
                p = get_cached_prediction(r['id'])
                if not p:
                    return None
                    
                confidence = p.get('confidence', 0)
                race_info = p.get('race_info', {})
                
                # --- おすすめスコアの算出 ---
                recommend_score = confidence  # ベースは既存の自信度
                reasons = []
                
                # 荒れる要素の加点
                h_count = len(p.get('horses', []))
                if h_count >= 16:
                    recommend_score += 20
                    reasons.append(f'多頭数({h_count}頭)で高配当期待')
                elif h_count >= 14:
                    recommend_score += 10
                    reasons.append(f'{h_count}頭立てで波乱含み')
                
                venue = r.get('venue', '')
                if venue in LOCAL_VENUES:
                    recommend_score += 10
                    reasons.append(f'ローカル({venue})で穴馬台頭の可能性')
                
                r_class = race_info.get('class', '') or race_info.get('class_name', '') or r.get('race_class', '')
                is_handicap = 'ハンデ' in r.get('name', '') or 'ハンデ' in r_class
                if is_handicap:
                    recommend_score += 15
                    reasons.append('ハンデ戦で実力差が縮まる')
                
                if '未勝利' in r_class or '1勝' in r_class or '条件' in r_class:
                    recommend_score += 10
                    reasons.append('下級条件戦は波乱傾向')
                
                if any(g in r_class for g in ['G1', 'G2', 'G3', '重賞']):
                    recommend_score -= 10
                    reasons.append('重賞は堅い決着も')
                
                # バグ度（人気と実力の乖離）が大きい馬が多いか
                horses = p.get('horses', [])
                bug_horses = 0
                for h in horses:
                    pop = h.get('popularity', 99)
                    omega = h.get('omega', 0)
                    if pop >= 6 and omega >= 60:
                        bug_horses += 1
                if bug_horses >= 2:
                    recommend_score += 10
                    reasons.append(f'穴馬に実力馬{bug_horses}頭（バグ馬多い）')
                
                # 除外条件：少頭数
                if h_count <= 10:
                    recommend_score -= 30
                    reasons.append('少頭数で配当期待薄')
                
                # 圧倒的1番人気チェック
                for h in horses:
                    pop = h.get('popularity', 0)
                    if pop == 1:
                        odds = h.get('popularity', 0)
                        # オッズ情報がある場合
                        break
                
                # 自信度加算（高いほどアプリの予測精度が良い）
                if confidence >= 70:
                    reasons.insert(0, f'AI自信度{confidence}で高精度期待')
                elif confidence >= 50:
                    reasons.insert(0, f'AI自信度{confidence}で安定予測')
                
                if not reasons:
                    reasons.append('総合条件から抽出')
                
                return {
                    'id': r['id'],
                    'race_num': r['race_num'],
                    'venue': venue or r.get('venue', ''),
                    'name': r.get('name', ''),
                    'time': r.get('time', ''),
                    'horse_count': h_count,
                    'surface': r.get('surface', ''),
                    'distance': r.get('distance', ''),
                    'race_class': r_class,
                    'confidence': confidence,
                    'recommend_score': recommend_score,
                    'reasons': reasons,
                    'recommendations': p.get('recommendations', [])[:3],
                    'himo_horses': p.get('himo_horses', [])[:5],
                    'discouraged': p.get('discouraged', [])[:3]
                }
            except Exception as e:
                print(f"Error analyzing Best5 race {r['id']}: {e}")
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            futures = [executor.submit(analyze_single, r) for r in races]
            for future in concurrent.futures.as_completed(futures):
                res_item = future.result()
                if res_item:
                    analyzed_races.append(res_item)
        
        # 3. 自信度（Confidence）でソートして上位5件を抽出
        analyzed_races.sort(key=lambda x: x.get('confidence', 0), reverse=True)
        best5 = analyzed_races[:5]
        
        # 全レースの自信度をマップとして作成（遅延反映用）
        all_confidences = {r['id']: r.get('confidence', 0) for r in analyzed_races}
        
        # 4. 過去日付の場合は結果データを付与
        is_past = False
        try:
            race_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
            today = datetime.date.today()
            is_past = race_date < today
        except:
            pass
        
        if is_past and best5:
            try:
                from result_scraper import ResultScraper
                r_scraper = ResultScraper()
                
                for race in best5:
                    try:
                        # 的中判定のために内部関数を利用
                        race_obj = next((r for r in races if r['id'] == race['id']), None)
                        if race_obj:
                            result = process_single_race_internal(
                                race_obj, date_str, r_scraper, scraper,
                                PREDICTION_CACHE, CACHE_LOCK, get_cached_prediction
                            )
                            if result:
                                race['result'] = {
                                    'hit_t46': result.get('hit_t46', False),
                                    'hit_t21': result.get('hit_t21', False),
                                    'hit_t16': result.get('hit_t16', False),
                                    'hit_t16m': result.get('hit_t16m', False),
                                    'hit_t10': result.get('hit_t10', False),
                                    'hit_u18': result.get('hit_u18', False),
                                    'hit_u13': result.get('hit_u13', False),
                                    'hit_u5': result.get('hit_u5', False),
                                    'pay_trio': result.get('pay_trio', 0),
                                    'pay_quinella': result.get('pay_quinella', 0),
                                    'top5': result.get('top5', []),
                                    'all8_cover': result.get('all8_cover_top3', False)
                                }
                    except Exception as e:
                        print(f"Result fetch error for {race['id']}: {e}")
            except ImportError:
                pass
        
        # 全体の回収率（過去日の場合）
        total_summary = None
        if is_past and best5:
            total_inv = 0
            total_ret = 0
            hits = 0
            for race in best5:
                r = race.get('result')
                if r:
                    total_inv += 4600  # 三連複46点 × 100円
                    if r.get('hit_t46'):
                        total_ret += r.get('pay_trio', 0)
                        hits += 1
            total_summary = {
                'total_races': len(best5),
                'hits': hits,
                'hit_rate': round(hits / len(best5) * 100, 1) if best5 else 0,
                'investment': total_inv,
                'return': total_ret,
                'roi': round(total_ret / total_inv * 100, 1) if total_inv > 0 else 0
            }
        
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] おすすめレース抽出完了 (TOP5)")
        
        return jsonify({
            'status': 'success',
            'date': date_str,
            'best_races': best5,
            'all_confidences': all_confidences,
            'venues': all_venues,
            'is_past': is_past,
            'total_summary': total_summary,
            'is_protected': scraper.is_protected
        })
        
    except Exception as e:
        err_msg = f"検証エラー: {str(e)}"
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Exception Error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'status': 'error', 'message': err_msg}), 500


@app.route('/api/reset_protection', methods=['POST'])
def reset_protection():
    """保護モード（サーキットブレーカー）を手動でリセットする"""
    was_protected = scraper.is_protected
    scraper.is_protected = False
    scraper.consecutive_errors = 0
    # キャッシュもクリアして新鮮な状態から再開
    with CACHE_LOCK:
        count = len(PREDICTION_CACHE)
        PREDICTION_CACHE.clear()
    print(f"[保護モードリセット] is_protected: {was_protected} -> False, キャッシュ {count}件 クリア")
    return jsonify({
        'status': 'success',
        'message': '保護モードを解除しました。再度分析をお試しください。',
        'was_protected': was_protected,
        'cache_cleared': count
    })

@app.route('/api/protection_status')
def protection_status():
    """現在の保護モード状態を返す"""
    return jsonify({
        'is_protected': scraper.is_protected,
        'consecutive_errors': scraper.consecutive_errors,
        'max_error_threshold': scraper.max_error_threshold
    })

if __name__ == '__main__':
    # Windowsでの安定動作のため debug=False, 外部アクセス許可のため host='0.0.0.0'
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)
