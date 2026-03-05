# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import math
import os
import json

class KeibaLabScraper:
    BASE_URL = "https://www.keibalab.jp"
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0'
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.consecutive_errors = 0
        self.max_error_threshold = 10
        self.is_protected = False
        self.last_request_time = 0

    VENUE_CODES = {
        '01': '札幌', '02': '函館', '03': '福島', '04': '新潟',
        '05': '東京', '06': '中山', '07': '中京', '08': '京都',
        '09': '阪神', '10': '小倉'
    }

    VENUE_ROTATION = {
        '東京': '左', '新潟': '左', '中京': '左',
        '中山': '右', '京都': '右', '阪神': '右',
        '札幌': '右', '函館': '右', '小倉': '右', '福島': '右'
    }

    # クラス別の「格」の重み付け
    GRADE_WEIGHTS = {
        'G1': 12.0, 'G2': 10.0, 'G3': 9.0, 'L': 8.0, 'OP': 7.5, 'オープン': 7.5,
        '3勝クラス': 6.0, '1600万下': 6.0,
        '2勝クラス': 5.0, '1000万下': 5.0,
        '1勝クラス': 4.0, '500万下': 4.0,
        '未勝利': 3.0, '新馬': 3.0
    }

    def _get_soup(self, url, headers=None):
        """指定されたURLを取得し、正しいエンコーディングでBeautifulSoupオブジェクトを返します。
           リトライロジック（最大3回）、Session維持、タイムアウト延長を導入。
           Safety Guard: 連続エラー検知による自動停止とアクセスペーシング。
        """
        import time
        import random
        
        if self.is_protected:
            print(f"[保護] サーキットブレーカー作動中のため、取得をスキップしました: {url}")
            return None

        # アクセスペーシング: 連続アクセスを避けるためにランダムな待機時間を挿入
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < 1.0: # 最低1秒は空ける
            wait = random.uniform(0.5, 1.5)
            time.sleep(wait)
        self.last_request_time = time.time()

        req_headers = self.HEADERS.copy()
        if headers:
            req_headers.update(headers)
        
        # 動的なリファラ設定
        if 'Referer' not in req_headers:
            req_headers['Referer'] = self.BASE_URL + "/"
            
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, headers=req_headers, timeout=25)
                
                if response.status_code == 403 or response.status_code == 429:
                    print(f"[警告] アクセス制限検知 (Status: {response.status_code}): {url}")
                    self.consecutive_errors += 1
                    if self.consecutive_errors >= self.max_error_threshold:
                        self.is_protected = True
                        print("[重大] 連続エラーが規定値を超えました。保護モードに移行します。")
                    raise requests.exceptions.RequestException(f"Access denied: {response.status_code}")
                
                if response.status_code != 200:
                    print(f"[警告] ステータスコード {response.status_code}: {url}")
                
                response.raise_for_status()
                
                # 成功した場合はエラーカウントをリセット
                self.consecutive_errors = 0
                
                content = response.content
                detected_enc = None
                
                # エンコーディング判定
                meta_charset = re.search(b'charset=["\']?([a-zA-Z0-9_-]+)', content[:2000], re.I)
                if meta_charset:
                    try:
                        detected_enc = meta_charset.group(1).decode('ascii').lower()
                        if detected_enc == 'shift_jis': detected_enc = 'cp932'
                    except: pass
                
                if not detected_enc:
                    for enc in ['utf-8', 'cp932', 'euc-jp']:
                        try:
                            content.decode(enc)
                            detected_enc = enc
                            break
                        except (AttributeError, UnicodeDecodeError):
                            continue
                
                if not detected_enc:
                    detected_enc = response.apparent_encoding
                
                response.encoding = detected_enc
                return BeautifulSoup(content, 'html.parser', from_encoding=detected_enc)

            except (requests.exceptions.RequestException) as e:
                self.consecutive_errors += 1
                if self.consecutive_errors >= self.max_error_threshold:
                    self.is_protected = True
                    print("[重大] 連続エラーが規定値を超えました。保護モードに移行します。")
                
                if attempt < max_retries - 1 and not self.is_protected:
                    # 指数バックオフ + ランダムジッター
                    wait_time = (2 ** attempt) * 2 + random.uniform(1.0, 3.0)
                    print(f"[再試行 {attempt+1}/{max_retries}] {url} の取得に失敗しました ({type(e).__name__}: {e})。{wait_time:.1f}秒後に再試行します...")
                    time.sleep(wait_time)
                else:
                    print(f"[エラー] {url} の取得に最終的に失敗しました: {e}")
                    return None
            except Exception as e:
                print(f"[エラー] {url} の取得中に予期せぬエラーが発生しました: {e}")
                return None

    def get_venue_from_id(self, race_id):
        if len(race_id) >= 12:
            venue_code = race_id[8:10]
            return self.VENUE_CODES.get(venue_code, '')
        return ''

    def get_races_by_date(self, date_str, skip_scoring=False):
        url = f"{self.BASE_URL}/db/race/{date_str.replace('-', '')}/"
        soup = self._get_soup(url)
        if not soup: return {'races': [], 'rough_races': [], 'stable_races': []}
        
        races = []
        try:
            race_links = soup.find_all('a', href=re.compile(r"/db/race/\d{10,}/"))
            race_data_map = {}
            
            for link in race_links:
                rid_match = re.search(r"/db/race/(\d{10,})/", link['href'])
                if not rid_match: continue
                rid = str(rid_match.group(1))
                
                if len(rid) < 12:
                    continue
                if rid[:8] != str(date_str).replace('-', ''):
                    continue

                text = link.get_text(strip=True)
                venue = self.get_venue_from_id(rid)
                race_num = rid[-2:].lstrip('0')
                
                race_class = ""
                surface = ""
                distance = ""
                
                row = link.find_parent(['tr', 'li', 'div'], class_=re.compile(r'race|link', re.I))
                if not row: row = link.find_parent('td') # 既存の fallback
                if not row: row = link.parent
                
                if row:
                    # 行全体のテキストを調べて、芝・ダート・距離の情報を探す
                    row_text = row.get_text(separator='|', strip=True)
                    # 芝1600 / ダ1200 / 障2800 などのパターンを探す
                    sd_match = re.search(r'([芝ダ障])(\d{3,4})', row_text)
                    if sd_match:
                        surface = sd_match.group(1).replace('ダ', 'ダート')
                        distance = sd_match.group(2) + "m"

                race_time = ""
                parent = link.find_parent('td')
                if parent:
                    # 1. セパレータ形式でテキストを全取得
                    texts = [t.strip() for t in parent.get_text(separator='|').split('|') if t.strip()]
                    
                    # 2. リンクテキストが単なる「14:20」のような時刻の場合、他の有用なテキストを探す
                    if re.match(r'^\d{1,2}:\d{2}$', text):
                        race_time = text
                        # 他のリンク（名前が入っている可能性が高い）を探す
                        other_links = parent.find_all('a')
                        for ol in other_links:
                            ol_text = ol.get_text(strip=True)
                            if ol_text and not re.match(r'^\d{1,2}:\d{2}$', ol_text) and not re.match(r'^\d{1,2}R$', ol_text):
                                text = ol_text
                                break
                    
                    for t in texts:
                        if re.match(r'^\d{1,2}:\d{2}$', t): 
                            race_time = t
                            continue
                        if re.match(r'^\d{1,2}R$', t): continue
                        if re.match(r'[芝ダ障]\d{3,4}', t): continue
                        if t: race_class = t; break
                
                if len(text) > len(race_class) and not re.match(r'^\d{1,2}R$', text) and not re.match(r'^\d{1,2}:\d{2}$', text):
                    race_class = text
                
                display_name = f"{venue}{race_num}R"
                if race_class: display_name += f" {race_class}"
                
                if rid not in race_data_map:
                    race_data_map[rid] = {
                        'id': rid, 'url': f"{self.BASE_URL}/db/race/{rid}/",
                        'name': display_name, 'venue': venue, 'time': race_time,
                        'race_num': int(race_num), 'race_class': race_class, 
                        'surface': surface, 'distance': distance,
                        'horse_count': 0, 'is_handicap': False, 'is_mare': False,
                        'weight_cond': '', 'is_race': True
                    }
                
                # 情報の統合（欠落している情報を補完）
                if race_time: race_data_map[rid]['time'] = race_time
                if len(display_name) > len(race_data_map[rid]['name']):
                    race_data_map[rid]['name'] = display_name
                if race_class and (not race_data_map[rid]['race_class'] or len(race_class) > len(race_data_map[rid]['race_class'])):
                    race_data_map[rid]['race_class'] = race_class
                if surface: race_data_map[rid]['surface'] = surface
                if distance: race_data_map[rid]['distance'] = distance
                
                # 頭数と条件の抽出
                if row:
                    row_text = row.get_text(separator='|', strip=True)
                    count_match = re.search(r'(\d+)頭', row_text)
                    if count_match:
                        race_data_map[rid]['horse_count'] = int(count_match.group(1))
                    
                    if "ハンデ" in row_text or "nf" in row_text:
                        race_data_map[rid]['is_handicap'] = True
                    
                    if "牝" in row_text:
                        race_data_map[rid]['is_mare'] = True

                    # 重量条件の抽出
                    weight_cond = ""
                    if "ハンデ" in row_text: weight_cond = "ハンデ"
                    elif "定量" in row_text: weight_cond = "定量"
                    elif "別定" in row_text: weight_cond = "別定"
                    elif "馬齢" in row_text: weight_cond = "馬齢"
                    if weight_cond:
                        race_data_map[rid]['weight_cond'] = weight_cond
            
            races = list(race_data_map.values())
            races.sort(key=lambda x: x['id'])

            if skip_scoring:
                return {
                    'races': races,
                    'rough_races': [],
                    'stable_races': []
                }

            # 注目レースのピックアップ (スコアリング)
            # 全レースを詳細チェックするのは重いので、上位条件や最終などを優先して指標を取得
            priority_races = sorted(races, key=lambda x: (x['race_num'] >= 10, any(c in x['race_class'] for c in ['G1', 'G2', 'G3', 'オープン'])), reverse=True)[:10]
            indicators_map = {}
            for pr in priority_races:
                indicators_map[pr['id']] = self._fetch_race_indicators(pr['id'])

            scored_rough = []
            scored_stable = []
            for r in races:
                inds = indicators_map.get(r['id'])
                # 波乱度
                r_score, r_reasons = self._calculate_roughness(r, inds)
                if r_score > 0:
                    scored_rough.append({**r, 'rough_score': r_score, 'rough_reasons': r_reasons})
                
                # 安定度
                s_score, s_reasons = self._calculate_stability(r, inds)
                if s_score > 0:
                    scored_stable.append({**r, 'stable_score': s_score, 'stable_reasons': s_reasons})
            
            # スコア順にソートしてそれぞれ上位5つ
            scored_rough.sort(key=lambda x: x['rough_score'], reverse=True)
            scored_stable.sort(key=lambda x: x['stable_score'], reverse=True)
            
            rough_races = scored_rough[:3]
            stable_races = scored_stable[:3]

            return {
                'races': races,
                'rough_races': rough_races,
                'stable_races': stable_races
            }
        except Exception as e:
            print(f"Error fetching race list: {e}")
            return {'races': [], 'rough_races': [], 'stable_races': []}

    def _calculate_stability(self, race, indicators=None):
        """レースの安定度（本命の評価しやすさ）をスコアリングする"""
        score = 0
        reasons = []
        
        # 1. 頭数と条件 (紛れが少ないか)
        horse_count = int(race.get('horse_count', 0))
        if 1 <= horse_count <= 10:
            score += 40
            reasons.append(f"{horse_count}頭の少頭数")
        elif horse_count <= 12:
            score += 20
            reasons.append("手頃な頭数")
            
        if not race.get('is_handicap'):
            score += 30
            reasons.append("能力通り決まりやすい定量・別定戦")
            
        # 2. 指標による真の実力評価
        if indicators:
            # Ω値でソート (indicatorsは名前, omega, jockey, is_top_jockey等を持つ)
            sorted_by_omega = sorted(indicators, key=lambda x: x.get('omega', 0), reverse=True)
            if sorted_by_omega:
                top_horse = sorted_by_omega[0]
                top_omega = float(top_horse.get('omega', 0))
                
                # Ω値が盤石か
                if top_omega >= 80:
                    score += 50
                    reasons.append(f"有力馬の指数({top_omega})が非常に高い")
                elif top_omega >= 70:
                    score += 25
                    reasons.append("有力馬が実力上位")
                
                # トップジョッキー騎乗 (実力を引き出しやすい)
                if top_horse.get('is_top_jockey'):
                    score += 30
                    reasons.append(f"実力馬に信頼のジョッキー({top_horse.get('jockey')})騎乗")
                
                # 2番手との能力差
                if len(sorted_by_omega) > 1:
                    second_omega = float(sorted_by_omega[1].get('omega', 0))
                    if (top_omega - second_omega) >= 10:
                        score += 25
                        reasons.append("1強ムード（指数が他を圧倒）")

        # 3. 条件・コース
        r_class = str(race.get('race_class', ''))
        venue = str(race.get('venue', ''))
        if any(x in r_class for x in ['G1', 'G2', 'G3', 'オープン', 'L']):
            score += 20
            reasons.append("実力馬が揃う上級条件")
        if venue in ['東京', '京都', '阪神', '中山']:
            score += 10
            reasons.append(f"有力馬が力を出しやすい中央主場")
            
        return score, reasons

    def _calculate_roughness(self, race, indicators=None):
        """レースの波乱度（荒れやすさ）をスコアリングする"""
        score = 0
        reasons = []
        
        # 1. ハンデ戦・多頭数 (波乱の土台)
        if race.get('is_handicap'):
            score += 40
            reasons.append("実力伯仲のハンデ戦")
            
        horse_count = int(race.get('horse_count', 0))
        if horse_count >= 16:
             score += 30
             reasons.append(f"{horse_count}頭の多頭数・フルゲート")
        elif horse_count >= 14:
            score += 15
            reasons.append(f"{horse_count}頭の混戦模様")
            
        # 2. 指標による波乱要素 (実力と人気のギャップを推計)
        if indicators:
            # Ω値でソート
            sorted_by_omega = sorted(indicators, key=lambda x: x.get('omega', 0), reverse=True)
            
            # 絶対的な強者が不在
            if not any(h.get('omega', 0) >= 70 for h in sorted_by_omega):
                score += 40
                reasons.append("突き抜けた実力馬が不在の混戦")
            
            # 「危ない人気馬」の推計: Ωが低いのにトップジョッキーが乗っている
            dangerous = [h for h in indicators if h.get('omega', 0) < 60 and h.get('is_top_jockey')]
            if dangerous:
                score += 30
                reasons.append(f"人気先行が懸念される有力鞍の低指数馬が存在")
            
            # 「隠れた実力馬」の推計: Ωが高いのにトップジョッキー以外が乗っている (人気薄になりやすい)
            hidden_gems = [h for h in indicators if h.get('omega', 0) >= 75 and not h.get('is_top_jockey')]
            if hidden_gems:
                score += 45
                reasons.append(f"指数上位だが人気薄が予想される伏兵が潜伏")

        # 3. 条件・コース
        r_class = str(race.get('race_class', ''))
        venue = str(race.get('venue', ''))
        dist_str = str(race.get('distance', ''))
        
        if "3歳未勝利" in r_class:
            score += 20
            reasons.append("能力比較が難しい3歳未勝利戦")
        if "牝" in r_class:
            score += 10
            reasons.append("波乱含みの牝馬限定戦")
        if "2歳" in r_class:
            score += 15
            reasons.append("キャリアの浅い2歳戦")

        # ローカル場 (紛れが発生しやすい)
        if venue in ['福島', '新潟', '小倉', '函館', '札幌']:
            score += 15
            reasons.append(f"紛れの生じやすいローカル{venue}開催")
            
        # 短距離戦
        try:
            dist_val = int(dist_str.replace('m', ''))
            if dist_val <= 1200:
                score += 10
                reasons.append(f"{dist_val}mの電撃短距離戦")
        except:
            pass
            
        return score, reasons

    def _fetch_race_indicators(self, race_id):
        """出馬表から馬の能力指標（Ω値、騎手、前走成績など）を多角的に取得する"""
        url = f"{self.BASE_URL}/db/race/{race_id}/umabashira.html"
        soup = self._get_soup(url)
        if not soup: return None
        
        indicators = []
        try:
            rows = soup.find_all('tr')
            if len(rows) < 20: return None
            
            # 行インデックスの特定 (今回の解析結果に基づく)
            # Row 3: 馬名, Row 10: 騎手, Row 12: Ω指数
            horse_names_row = rows[3].find_all('td') if len(rows) > 3 else []
            jockeys_row = rows[10].find_all('td') if len(rows) > 10 else []
            omega_row = rows[12].find_all('td') if len(rows) > 12 else []
            zensou_row = rows[20].find_all('td') if len(rows) > 20 else [] # 1頭につき複数TD使用
            
            horse_count = len(horse_names_row) - 3 # ヘッダー等を除く実数
            if horse_count <= 0: return None

            top_jockeys = ['ルメール', '川田', '武豊', '戸崎', '横山武', '坂井', '鮫島克', '松山', 'モレイラ', 'レーン']

            for i in range(2, 2 + horse_count):
                name = horse_names_row[i].get_text(strip=True)
                jockey = jockeys_row[i].get_text(strip=True)
                omega_str = omega_row[i].get_text(strip=True)
                
                omega = 0
                try: omega = float(omega_str)
                except: pass
                
                # 人気予測・実績
                is_top_jockey = any(tj in jockey for tj in top_jockeys)
                
                # 前走着順 (zensou_row は 1頭につき通常9TD block)
                # i=2 -> TD 2, i=3 -> TD11, i=4 -> TD20 ... (base 2, step 9)
                z_idx = 2 + (i-2) * 9
                last_rank = 99
                if z_idx < len(zensou_row):
                    z_text = zensou_row[z_idx].get_text(strip=True)
                    # "1人1:46.9" 形式から着順を推測 (文字化け耐性のため正規表現)
                    rank_match = re.search(r'(\d+)着|(\d+)人', z_text) # 暫定
                
                # 実データとして保存
                indicators.append({
                    'name': name,
                    'omega': omega,
                    'jockey': jockey,
                    'is_top_jockey': is_top_jockey,
                    'popularity': 99 # 人気は不明な場合が多い
                })
        except Exception as e:
            # print(f"Indicator fetch error: {e}")
            pass
            
        return indicators

    def get_race_details(self, race_id):
        base = f"{self.BASE_URL}/db/race/{race_id}"
        return {
            'umabashira': f"{base}/umabashira.html",
            'course': f"{base}/course.html",
            'blood': f"{base}/blood.html",
            'past': f"{base}/past.html",
            'odds': f"{base}/odds.html"
        }

    def scrape_race_info(self, race_id):
        url = f"{self.BASE_URL}/db/race/{race_id}/"
        soup = self._get_soup(url)
        if not soup:
            return {'venue': '', 'distance': '0', 'surface': '芝', 'race_name': '', 'class_name': '', 'conditions': ''}

        venue = self.get_venue_from_id(race_id)
        full_text = soup.get_text()
        
        dist_surf_text = ""
        data_header = (soup.find(class_='racedatawrap') or 
                       soup.find(class_=re.compile(r'raceData|courseInfo', re.I)))
        if data_header: dist_surf_text = data_header.get_text()
        else:
            match = re.search(r'(芝|ダ|障).*?(\d+)m', full_text)
            if match: dist_surf_text = match.group(0)

        surface = '芝' if '芝' in dist_surf_text else 'ダート' if 'ダ' in dist_surf_text else '芝'
        distance = '0'
        dist_match = re.search(r'(\d+)m', dist_surf_text)
        if dist_match: 
            distance = dist_match.group(1)
        else:
            dist_match = re.search(r'(\d+)', dist_surf_text)
            if dist_match: distance = dist_match.group(1)
            
        race_name = ''
        for h1 in soup.find_all('h1'):
            txt = h1.get_text(strip=True)
            if txt:
                race_name = txt
                break
        if not race_name:
            name_elem = soup.find('div', class_='raceName')
            race_name = name_elem.get_text(strip=True) if name_elem else ''

        class_name = ''
        conditions = ''
        if data_header:
            header_text = data_header.get_text(strip=True)
            class_match = re.search(r'(新馬|未勝利|1勝クラス|2勝クラス|3勝クラス|オープン|\(L\)|\(G[1-3I-III]+\))', header_text)
            if class_match:
                class_name = class_match.group(1)
            
            cond_list = []
            if '牝' in header_text: cond_list.append('牝')
            if 'ハンデ' in header_text: cond_list.append('ハンデ')
            elif '別定' in header_text: cond_list.append('別定')
            elif '馬齢' in header_text: cond_list.append('馬齢')
            elif '定量' in header_text: cond_list.append('定量')
            conditions = ' '.join(cond_list)

        # 馬場状態の抽出 (良, 稍重, 重, 不良)
        track_condition = '良'
        tc_match = re.search(r'(良|稍重|重|不良)', dist_surf_text)
        if tc_match:
            track_condition = tc_match.group(1)
        elif '馬場:' in full_text:
            tc_match = re.search(r'馬場:(良|稍重|重|不良)', full_text)
            if tc_match: track_condition = tc_match.group(1)

        return {
            'venue': venue, 'distance': distance, 'surface': surface, 'race_name': race_name,
            'class_name': class_name, 'conditions': conditions, 'track_condition': track_condition
        }

    def scrape_odds(self, race_id):
        url = f"{self.BASE_URL}/db/race/{race_id}/odds.html"
        soup = self._get_soup(url)
        if not soup: return {}
        popularity = {}
        try:
            table = soup.find('table', class_='oddsTable')
            if table:
                for row in table.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        h_num = cols[0].get_text(strip=True)
                        pop_rank = cols[2].get_text(strip=True)
                        if h_num.isdigit() and pop_rank.isdigit():
                            popularity[int(h_num)] = int(pop_rank)
        except: pass
        return popularity

    def scrape_race_card(self, race_id):
        url = f"{self.BASE_URL}/db/race/{race_id}/syutsuba.html"
        soup = self._get_soup(url)
        if not soup: return self._scrape_race_card_fallback(race_id)
            
        horses = []
        try:
            table = soup.find('table', class_='all_umabashira') or soup.find('table', class_='DbTable2')
            if not table: return self._scrape_race_card_fallback(race_id)

            header_row = table.find('tr')
            th_cells = header_row.find_all(['th', 'td'])
            col_map = {}
            for idx, th in enumerate(th_cells):
                txt = th.get_text(strip=True)
                # 「馬番」を先にチェック（「番」だけだと「枠」列にもマッチする）
                if txt == '枠': col_map['frame'] = idx
                elif '馬番' in txt or (txt == '番' and 'frame' in col_map): col_map['num'] = idx
                elif '番' in txt and 'frame' not in col_map and 'num' not in col_map: col_map['num'] = idx
                elif '馬名' in txt: col_map['name'] = idx
                elif '斤量' in txt: col_map['kinryo'] = idx
                elif '騎手' in txt: col_map['jockey'] = idx
                elif '馬体重' in txt: col_map['weight'] = idx
                elif '間隔' in txt: col_map['interval'] = idx
                elif '人気' in txt: col_map['popularity'] = idx
                elif 'Ω' in txt: col_map['omega'] = idx

            idx_frame = col_map.get('frame', 0)
            idx_num = col_map.get('num', 1)
            idx_name = col_map.get('name', 3)
            idx_kinryo = col_map.get('kinryo', 10)
            idx_jockey = col_map.get('jockey', 6)
            idx_weight = col_map.get('weight', 14)
            idx_interval = col_map.get('interval', 9)
            idx_pop = col_map.get('popularity', 13)
            idx_omega = col_map.get('omega', 7)

            for row in table.find_all('tr')[1:]:
                tds = row.find_all('td')
                if len(tds) < 5: continue
                h_name_cell = row.find('a', href=re.compile(r"/db/horse/\d+/"))
                if not h_name_cell: continue
                
                h_id = h_name_cell['href'].split('/')[-2]
                h_name = h_name_cell.get_text(strip=True)
                
                raw_num = tds[idx_num].get_text(strip=True)
                h_num_match = re.search(r'(\d+)', raw_num)
                h_num = int(h_num_match.group(1)) if h_num_match else 0
                
                # 枠番を取得
                frame = 0
                try:
                    frame_txt = tds[idx_frame].get_text(strip=True)
                    frame_match = re.search(r'(\d+)', frame_txt)
                    if frame_match: frame = int(frame_match.group(1))
                except: pass
                
                # 間隔の取得 (全tdからキーワード検索するヒューリスティック)
                interval = '-'
                for td in tds:
                    t = td.get_text(strip=True)
                    if ('中' in t and '週' in t) or '連闘' in t:
                        interval = t
                        break
                
                # 騎手の取得 (リンクから)
                j_link = row.find('a', href=re.compile(r"/db/jockey/\d+/"))
                jockey = j_link.get_text(strip=True) if j_link else tds[idx_jockey].get_text(strip=True) if len(tds) > idx_jockey else '-'
                j_id = ''
                if j_link:
                    j_id_match = re.search(r'/jockey/(\d+)/', j_link['href'])
                    if j_id_match: j_id = j_id_match.group(1)
                
                kinryo = tds[idx_kinryo].get_text(strip=True) if len(tds) > idx_kinryo else '-'
                bw_info = tds[idx_weight].get_text(strip=True) if len(tds) > idx_weight else '-'
                
                pop = 0
                try:
                    p_txt = tds[idx_pop].get_text(strip=True) if len(tds) > idx_pop else ''
                    p_match = re.search(r'(\d+)', p_txt)
                    if p_match: pop = int(p_match.group(1))
                except: pass
                
                omega = 0.0
                if idx_omega != -1 and len(tds) > idx_omega:
                    try:
                        o_txt = re.sub(r'[^\d\.]', '', tds[idx_omega].get_text(strip=True))
                        if o_txt: omega = float(o_txt)
                    except: pass

                horses.append({
                    'horse_id': h_id, 'horse_name': h_name, 'horse_url': f"{self.BASE_URL}/db/horse/{h_id}/",
                    'number': h_num, 'frame': frame,
                    'jockey': jockey, 'jockey_id': j_id, 'kinryo': kinryo, 'body_weight_info': bw_info,
                    'interval': interval, 'popularity': pop, 'omega': omega, 'blinker': False
                })
            
            try:
                u_soup = self._get_soup(f"{self.BASE_URL}/db/race/{race_id}/umabashira.html")
                if u_soup:
                    # 馬柱から間隔データをカラムインデックスベースで取得
                    uma_table = u_soup.find('table', class_='megamoriTable')
                    if uma_table:
                        # 1. 各馬のカラムインデックスを特定
                        horse_col_map = {}
                        tr_list = uma_table.find_all('tr') if uma_table else []
                        for tr in tr_list:
                            td_list = tr.find_all(['td', 'th'])
                            for idx, td in enumerate(td_list):
                                link = td.find('a', href=re.compile(r'/db/horse/(\d+)/'))
                                if link:
                                    h_id_match = re.search(r'/db/horse/(\d+)/', link['href'])
                                    if h_id_match:
                                        horse_col_map[h_id_match.group(1)] = idx
                        
                        # 2. 「間隔」行を特定 (最後が「間」を含む、または中x週が含まれる行)
                        int_row = None
                        tr_list_2 = uma_table.find_all('tr') if uma_table else []
                        for tr in tr_list_2:
                            tds_2 = tr.find_all(['td', 'th'])
                            if not tds_2: continue
                            last_txt = tds_2[-1].get_text(strip=True)
                            # 「間隔」の文字化け（Ԋu）や「間」を考慮
                            if '間' in last_txt or any('週' in td.get_text() for td in tds_2):
                                # 全体的に「週」が含まれる数が多い行を優先（誤判定防止）
                                week_count = sum(1 for td in tds_2 if '週' in td.get_text())
                                if week_count >= len(horses) // 2:
                                    int_row = tr
                                    break
                        
                        # 3. 各馬にデータをセット
                        if int_row:
                            int_tds = int_row.find_all(['td', 'th'])
                            for h in horses:
                                h_id = h['horse_id']
                                if h_id in horse_col_map:
                                    col_idx = horse_col_map[h_id]
                                    if col_idx < len(int_tds):
                                        h['interval'] = int_tds[col_idx].get_text(strip=True)

                        # ブリンカーと枠番の取得（従来通り）
                        for h in horses:
                            link = u_soup.find('a', href=re.compile(rf"/db/horse/{h['horse_id']}/"))
                            if link:
                                parent = link.find_parent('td')
                                if parent and 'B' in parent.get_text(): h['blinker'] = True
                                
                                grandparent = parent.parent if parent else None
                                if grandparent:
                                    waku_cls = grandparent.find(class_=re.compile(r'waku\d'))
                                    if waku_cls: 
                                        w_match = re.search(r'waku(\d)', str(waku_cls))
                                        if w_match: h['frame'] = int(w_match.group(1))
            except Exception as e:
                print(f"Error in uma_interval extraction: {e}")
            return horses
        except Exception as e:
            print(f"Error scraping race card: {e}")
            return []

    def _scrape_race_card_fallback(self, race_id):
        url = f"{self.BASE_URL}/db/race/{race_id}/umabashira.html"
        soup = self._get_soup(url)
        if not soup: return []
        horses = []
        seen = set()
        links = soup.find_all('a', href=re.compile(r"/db/horse/\d+/"))
        for link in links:
            hid = link['href'].split('/')[-2]
            if hid in seen: continue
            seen.add(hid)
            name = link.get_text(strip=True)
            if not name: continue
            h_num = '0'
            parent = link.find_parent('td')
            if parent:
                prev = parent.find_previous_sibling('td')
                if prev and prev.get_text(strip=True).isdigit(): h_num = prev.get_text(strip=True)
            horses.append({
                'horse_id': hid, 'horse_name': name, 'horse_url': f"{self.BASE_URL}/db/horse/{hid}/",
                'number': int(h_num), 'frame': 0, 'jockey': '', 'kinryo': '', 'body_weight_info': '-',
                'interval': '-', 'popularity': 0, 'omega': 0.0, 'blinker': False
            })
        return horses

    def scrape_horse_details(self, horse_id):
        """馬の全レース履歴ページをスクレイピングします。"""
        url = f"{self.BASE_URL}/db/horse/{horse_id}/"
        soup = self._get_soup(url)
        if not soup: return []
        
        history = []
        table = (soup.find('table', id='HorseResultTable') or 
                 soup.find('table', class_='dbRaceList') or 
                 soup.find('table', id='dbRaceList'))
        
        if not table:
            for t in soup.find_all('table'):
                rows = t.find_all('tr')
                if not rows: continue
                header_text = rows[0].get_text()
                if any(x in header_text for x in ['着', '日付', '開催']):
                    table = t
                    break
        
        if not table: return []
        
        rows = table.find_all('tr')
        if len(rows) < 2: return []
        
        header_cells = rows[0].find_all(['th', 'td'])
        col = {}
        for i, cell in enumerate(header_cells):
            h = cell.get_text(strip=True)
            if re.search(r'年月日|日付', h): col['date'] = i
            elif re.search(r'開催|^場$', h): col['venue'] = i
            elif re.search(r'コース|距離', h): col['course'] = i
            elif re.search(r'レース|Race', h): col['race'] = i
            elif re.search(r'人気', h): col['pop'] = i
            elif re.search(r'^着$|^着順$', h): col['rank'] = i
            elif re.search(r'騎手', h): col['jockey'] = i
            elif re.search(r'斤量', h): col['kinryo'] = i
            elif re.search(r'枠', h): col['frame'] = i
            elif re.search(r'番', h): col['horse_num'] = i
            elif re.search(r'タイム', h): col['time'] = i
            elif re.search(r'馬体重', h): col['bweight'] = i
            elif re.search(r'通過', h): col['pos'] = i
            elif re.search(r'馬場', h): col['cond'] = i
            elif re.search(r'上|3F', h): col['last3f'] = i
            elif h == 'B': col['blinker'] = i
            elif re.search(r'ペース', h): col['pace'] = i
            elif re.search(r'着差', h): col['diff'] = i

        def safe_get(key, fallback_idx, data_row):
            idx = col.get(key, fallback_idx)
            return data_row[idx] if 0 <= idx < len(data_row) else ''

        for row in rows[1:]:
            tds = row.find_all('td')
            if len(tds) < 5: continue
            # get_text(strip=True) だと 3 <br> 3 が 33 になるため、separator を入れる
            data = [re.sub(r'\s+', ' ', c.get_text(separator=' ', strip=True)) for c in tds]
            
            date_str = safe_get('date', 0, data)
            venue_raw = safe_get('venue', 1, data)
            course_raw = safe_get('course', 2, data)
            rank_str = safe_get('rank', 7, data)
            
            rank = 99
            try:
                rank_digits = re.sub(r'\D', '', rank_str)
                if rank_digits: rank = int(rank_digits)
            except: pass
            
            time_str = safe_get('time', 13, data)
            time_val = None
            try:
                if ':' in time_str:
                    m, s = time_str.split(':')
                    time_val = float(m) * 60 + float(s)
                elif time_str:
                    time_val = float(time_str)
            except: pass

            jockey_td = tds[col.get('jockey', 8)] if 0 <= col.get('jockey', 8) < len(tds) else None
            jockey = re.sub(r'\s+', ' ', jockey_td.get_text(strip=True)) if jockey_td else '-'
            j_id = ''
            if jockey_td:
                j_link = jockey_td.find('a', href=re.compile(r"/db/jockey/\d+/"))
                if j_link:
                    j_id_match = re.search(r'/jockey/(\d+)/', j_link['href'])
                    if j_id_match: j_id = j_id_match.group(1)

            kinryo = safe_get('jockey', 8, data) # Wait, let's fix the safe_get call if needed, but safe_get('kinryo') is better
            kinryo = safe_get('kinryo', 9, data)
            body_w_raw = safe_get('bweight', 18, data)
            body_weight = 0
            bw_match = re.search(r'(\d+)', body_w_raw)
            if bw_match: body_weight = int(bw_match.group(1))

            pace_raw = safe_get('pace', 15, data)
            pace = ''
            # ペースは「35.1 - 34.1(S)」のように末尾に(S)/(M)/(H)が付く形式
            pace_match = re.search(r'\(([SsMmHh])\)\s*$', pace_raw)
            if not pace_match:
                # フォールバック: 末尾の単独文字
                pace_match = re.search(r'([SsMmHh])\s*$', pace_raw)
            if not pace_match: 
                # フォールバック: 全データから探す
                pace_match = re.search(r'\(([SsMmHh])\)', ' '.join(data))
            if pace_match: pace = pace_match.group(1).upper()

            frame = 0
            f_match = re.search(r'(\d+)', safe_get('frame', 11, data))
            if f_match: frame = int(f_match.group(1))

            h_num_past = 0
            h_match = re.search(r'(\d+)', safe_get('horse_num', 12, data))
            if h_match: h_num_past = int(h_match.group(1))

            last_3f = safe_get('last3f', 16, data).strip()
            if not last_3f: last_3f = '99.9'
            
            blinker = True if safe_get('blinker', 17, data) == 'B' else False

            positions_raw = safe_get('pos', 19, data)
            running_style = ''
            if positions_raw:
                pos_list = re.findall(r'(\d+|[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱])', positions_raw)
                if pos_list:
                    pos_map = '①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱'
                    first_pos = 99
                    if pos_list[0] in pos_map:
                        first_pos = pos_map.index(pos_list[0]) + 1
                    elif pos_list[0].isdigit():
                        first_pos = int(pos_list[0])
                    
                    if first_pos <= 2: running_style = '逃げ' if first_pos == 1 else '先行'
                    elif first_pos <= 6: running_style = '先行'
                    elif first_pos <= 12: running_style = '差し'
                    else: running_style = '追込'

            surface = '芝' if any(x in course_raw for x in ['芝', 'Turf']) else 'ダート' if any(x in course_raw for x in ['ダ', 'Dirt']) else ''
            dist_match = re.search(r'(\d+)', course_raw)
            distance = dist_match.group(1) if dist_match else ''
            
            venue_clean = re.sub(r'[0-9]', '', venue_raw).strip()
            for v_name in self.VENUE_CODES.values():
                if v_name in venue_raw:
                    venue_clean = v_name
                    break

            pop = 0
            pop_match = re.search(r'(\d+)', safe_get('pop', 6, data))
            if pop_match: pop = int(pop_match.group(1))

            condition = safe_get('cond', 4, data)

            diff_str = safe_get('diff', -1, data)
            time_diff = 0.0
            try:
                # "+0.5" などの形式を数値化
                diff_match = re.search(r'([\+\-]?\d+\.?\d*)', diff_str)
                if diff_match: time_diff = float(diff_match.group(1))
            except: pass

            race_name = safe_get('race', 5, data)
            # クラス名（格）の抽出
            race_class = ''
            class_match = re.search(r'(\(G[1-3]\)|G[1-3]|\(L\)|オープン|OP|1勝クラス|2勝クラス|3勝クラス|未勝利|新馬)', race_name)
            if class_match:
                race_class = class_match.group(1).replace('(', '').replace(')', '')
            else:
                # 重賞以外でも「万葉S(OP)」などの形式があるため、別途検索
                if 'オープン' in race_name or 'OP' in race_name: race_class = 'OP'
                elif '1勝' in race_name: race_class = '1勝クラス'
                elif '2勝' in race_name: race_class = '2勝クラス'
                elif '3勝' in race_name: race_class = '3勝クラス'
                elif '未勝利' in race_name: race_class = '未勝利'
                elif '新馬' in race_name: race_class = '新馬'

            history.append({
                'date': date_str, 'venue': venue_clean, 'venue_raw': venue_raw,
                'course_raw': course_raw, 'surface': surface, 'distance': distance,
                'rank': rank, 'time': time_val, 'time_str': time_str, 
                'time_diff': time_diff, # 着差データを追加
                'jockey': jockey, 'jockey_id': j_id,
                'kinryo': kinryo, 'body_weight': body_weight, 'body_weight_str': body_w_raw,
                'pace': pace, 'frame': frame, 'horse_num': h_num_past,
                'running_style': running_style, 'race_name': race_name,
                'race_class': race_class, # クラス情報を追加
                'pop': pop, 'condition': condition, 'blinker': blinker, 'last_3f': last_3f,
                'positions': positions_raw
            })
        return history

    def scrape_data_page(self, horse_id):
        return self._get_soup(f"{self.BASE_URL}/db/horse/{horse_id}/data.html")

    def scrape_interval_stats(self, horse_id, soup=None):
        stats = {}
        if not soup: soup = self.scrape_data_page(horse_id)
        if not soup: return stats
        
        header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4'] and 
                          'レース間隔別' in tag.get_text() and 
                          'heading01' in tag.get('class', []))
        if not header:
            header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4', 'div'] and 'レース間隔別' in tag.get_text())
            
        if header:
            table = header.find_next('table')
            if table:
                for row in table.find_all('tr')[1:]:
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        label = tds[0].get_text(strip=True)
                        val = tds[1].get_text(strip=True)
                        stats[label] = val
        return stats

    def scrape_style_stats(self, horse_id, soup=None):
        stats = {}
        if not soup: soup = self.scrape_data_page(horse_id)
        if not soup: return stats
        
        header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4'] and 
                          '脚質別' in tag.get_text() and 
                          'heading01' in tag.get('class', []))
        if not header:
            header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4', 'div'] and '脚質別' in tag.get_text())
            
        if header:
            table = header.find_next('table')
            if table:
                for row in table.find_all('tr')[1:]:
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        label = tds[0].get_text(strip=True)
                        val = tds[1].get_text(strip=True)
                        stats[label] = val
        return stats

    def scrape_kinryo_stats(self, horse_id, target_kinryo, soup=None):
        """斤量別成績を取得する。キーは範囲形式（例: '53.5～55kg'）"""
        if not soup: soup = self.scrape_data_page(horse_id)
        if not soup: return '-'
        
        header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4'] and 
                          '斤量' in tag.get_text() and 
                          'heading01' in tag.get('class', []))
        if not header:
            header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4', 'div'] and '斤量' in tag.get_text())
            
        if header:
            table = header.find_next('table')
            if table:
                tk_match = re.search(r'(\d+\.?\d*)', str(target_kinryo))
                if not tk_match: return '-'
                tk_val = float(tk_match.group(1))
                
                for row in table.find_all('tr')[1:]:
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        label = tds[0].get_text(strip=True)
                        # 範囲形式: "53.5～55kg" or "59.5kg～"
                        nums = re.findall(r'(\d+\.?\d*)', label)
                        if len(nums) >= 2:
                            lo, hi = float(nums[0]), float(nums[1])
                            if lo <= tk_val <= hi:
                                return tds[1].get_text(strip=True)
                        elif len(nums) == 1:
                            # "59.5kg～" のような片側範囲
                            val = float(nums[0])
                            if '～' in label or '以上' in label or '~' in label:
                                if tk_val >= val:
                                    return tds[1].get_text(strip=True)
        return '-'

    def scrape_frame_stats(self, horse_id, soup=None):
        """枠番別成績を取得する。"""
        stats = {}
        if not soup: soup = self.scrape_data_page(horse_id)
        if not soup: return stats
        
        # ヘッダーは「枠番別」（「枠順別」ではない）
        header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4'] and 
                          '枠番' in tag.get_text() and 
                          'heading01' in tag.get('class', []))
        if not header:
            header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4', 'div'] and '枠番' in tag.get_text())
            
        if header:
            table = header.find_next('table')
            if table:
                for row in table.find_all('tr')[1:]:
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        label = tds[0].get_text(strip=True)
                        val = tds[1].get_text(strip=True)
                        if label.isdigit():
                            stats[label] = val
        return stats

    def predict_pace_from_list(self, pre_data):
        """Phase 1で収集したデータを元にペースを予測する。"""
        styles = {'逃げ': 0, '先行': 0, '差し': 0, '追込': 0}
        total = 0
        for hd in pre_data:
            rs = hd.get('running_style', '')
            if rs in styles: styles[rs] += 1; total += 1
        
        if total == 0: return {'pace': 'M', 'reason': 'データ不足', 'eliminated': []}
        
        ratio = (styles['逃げ'] + styles['先行']) / total
        nige = styles['逃げ']
        
        pace = 'M'
        reason = ''
        if nige >= 3 or ratio >= 0.6: 
            pace = 'H'
            reason = f"逃げ{nige}・先行{styles['先行']}。ハイペース想定"
        elif nige <= 1 and ratio <= 0.3: 
            pace = 'S'
            reason = f"逃げ{nige}・先行{styles['先行']}。スローペース想定"
        else: 
            reason = f"逃げ{nige}・先行{styles['先行']}・差し{styles['差し']}平均ペース"
        
        eliminated = []
        for hd in pre_data:
            pop = hd.get('popularity', 0)
            rs = hd.get('running_style', '')
            name = hd.get('name', '')
            if 0 < pop <= 3:
                if (pace == 'H' and rs in ['逃げ', '先行']) or (pace == 'S' and rs in ['差し', '追込']):
                    eliminated.append(f"{name}({pop}人・{rs})")
        
        return {'pace': pace, 'reason': reason, 'eliminated': eliminated, 'style_counts': styles}

    def _analyze_disregard_reasons(self, history, current_style, cur_date_str, cur_dist=None, cur_surface=None, cur_kinryo=None):
        """直近3走の敗因・勝因をデータから推測します。"""
        reasons = []
        detailed_reasons = [] # ロジック用
        labels = ["前", "2前", "3前"]
        idx_count = 0
        for r in history:
            if r.get('date') == cur_date_str: continue
            if idx_count >= 3: break
            
            race_reasons = []
            race_detailed = {'rank': r.get('rank', 99), 'reasons': []}
            
            rank = r.get('rank', 99)
            pace = r.get('pace', '')
            positions = r.get('positions', '')
            cond = r.get('condition', '')
            dist = r.get('distance', '')
            
            # 出遅れ判定 (逃げ・先行馬が1角で後方)
            if current_style in ['逃げ', '先行'] and positions:
                first_pos_match = re.search(r'(\d+)', positions)
                if first_pos_match:
                    first_pos = int(first_pos_match.group(1))
                    if first_pos >= 10:
                        race_reasons.append("出遅れ気味")
                        race_detailed['reasons'].append("gate_miss")
            
            # 展開不向き
            if rank > 3:
                if pace == 'S' and current_style in ['差し', '追込']:
                    race_reasons.append("展開不向き(S)")
                    race_detailed['reasons'].append("pace_unfit_s")
                elif pace == 'H' and current_style in ['逃げ', '先行']:
                    race_reasons.append("展開不向き(H)")
                    race_detailed['reasons'].append("pace_unfit_h")
            
            # 距離不向き
            if cur_dist and dist and dist != cur_dist and rank > 5:
                race_reasons.append("距離不適?")
                race_detailed['reasons'].append("dist_unfit")

            # 特殊馬場
            if rank > 5 and any(x in cond for x in ['重', '不']):
                race_reasons.append("馬場重い")
                race_detailed['reasons'].append("track_condition")
            
            if rank == 1:
                race_reasons.append("快勝")
            elif rank <= 3:
                race_reasons.append("善戦")

            if race_reasons:
                reasons.append(f"{labels[idx_count]}:{','.join(race_reasons)}")
            
            detailed_reasons.append(race_detailed)
            idx_count += 1
            
        return " / ".join(reasons) if reasons else "-", detailed_reasons

    def _evaluate_buyability(self, detailed_reasons, cur_condition, cur_dist, cur_surface, predicted_pace, current_style):
        """今回買えるかどうかを総合的に判定します。"""
        score = 0
        positive_factors = []
        negative_factors = []
        
        if not detailed_reasons:
            return "判定不能", "データ不足"

        last_race = detailed_reasons[0]
        
        # 1. 前走敗因の解消チェック
        for reason in last_race['reasons']:
            if reason == "track_condition" and "良" in cur_condition:
                positive_factors.append("馬場回復良")
                score += 2
            if reason == "pace_unfit_s" and predicted_pace in ['M', 'H']:
                positive_factors.append("展開好転想定")
                score += 2
            if reason == "pace_unfit_h" and predicted_pace in ['M', 'S']:
                positive_factors.append("展開好転想定")
                score += 2
            if reason == "dist_unfit":
                positive_factors.append("距離短縮/戻り")
                score += 2

        # 2. 展開相性
        if predicted_pace == 'H' and current_style in ['差し', '追込']:
            positive_factors.append("ハイペース差し向き")
            score += 1
        elif predicted_pace == 'S' and current_style in ['逃げ', '先行']:
            positive_factors.append("スロー逃げ先行有利")
            score += 1

        # 3. 前走着順
        if last_race['rank'] <= 3:
            positive_factors.append("前走好走")
            score += 1
        elif last_race['rank'] >= 10:
            negative_factors.append("前走大敗")
            score -= 1

        # 総合判定
        if score >= 3:
            result = "買い"
        elif score >= 1:
            result = "検討"
        elif score <= -1:
            result = "見送り"
        else:
            result = "中立"
            
        reason_str = ""
        if positive_factors:
            reason_str += "プラス:" + ",".join(positive_factors)
        if negative_factors:
            if reason_str: reason_str += " / "
            reason_str += "マイナス:" + ",".join(negative_factors)
            
        return result, (reason_str if reason_str else "目立つ材料なし")

    def scrape_course_info(self, race_id):
        """コース解説ページをスクレイピングして特性（買いの法則等）を取得します。"""
        url = f"{self.BASE_URL}/db/race/{race_id}/course.html"
        try:
            res = requests.get(url, headers=self.HEADERS, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.content, 'html.parser')
            
            chars = {
                'fav_jockeys': [],
                'fav_trainers': [],
                'fav_sires': [],
                'fav_gates': []
            }
            
            # 「買いの法則」セクションを探す
            law_sec = None
            for h2 in soup.find_all('h2'):
                if '買いの法則' in h2.text:
                    h2_parent = h2.find_parent('div', class_='courseAnalysis')
                    law_sec = h2_parent if h2_parent else h2.parent
                    break
            
            if law_sec:
                # 騎手
                j_labels = law_sec.find_all('h5', string=re.compile('買い騎手'))
                for lb in j_labels:
                    siblings = lb.find_next_siblings('a')
                    for a in siblings:
                        if a.name == 'a': chars['fav_jockeys'].append(a.text.strip())
                
                # 調教師
                t_labels = law_sec.find_all('h5', string=re.compile('買い調教師'))
                for lb in t_labels:
                    siblings = lb.find_next_siblings('a')
                    for a in siblings:
                        if a.name == 'a': chars['fav_trainers'].append(a.text.strip())

                # 種牡馬
                s_labels = law_sec.find_all('h5', string=re.compile('買い種牡馬'))
                for lb in s_labels:
                    siblings = lb.find_next_siblings('a')
                    for a in siblings:
                        if a.name == 'a': chars['fav_sires'].append(a.text.strip())
                
                # 枠
                g_labels = law_sec.find_all('h5', string=re.compile('買い枠'))
                for lb in g_labels:
                    txt_node = lb.find_next_sibling(string=True)
                    if txt_node:
                        txt_val = str(txt_node)
                        match = re.search(r'(\d+)枠', txt_val)
                        if match: chars['fav_gates'].append(match.group(1))
            
            return chars
        except Exception as e:
            print(f"Course scraping error: {e}")
            return {'fav_jockeys':[], 'fav_trainers':[], 'fav_sires':[], 'fav_gates':[]}

    def _analyze_weight_evaluation(self, bw_info, history):
        """馬体重の増減と過去の推移から評価を生成します。"""
        if not bw_info or bw_info == '-' or '計不' in bw_info or '(---)' in bw_info:
            return "-", "bw-neutral"

        # "480(-2)" のような形式をパース
        match = re.search(r'(\d+)\(([\+\-]?\d+)\)', bw_info)
        if not match:
            # 増減なしの場合 "480(0)" ではなく "480" だけの可能性も考慮
            match_no_diff = re.search(r'^(\d+)$', bw_info)
            if match_no_diff:
                weight = int(match_no_diff.group(1))
                diff = 0
            else:
                return bw_info, "bw-neutral"
        else:
            weight = int(match.group(1))
            diff = int(match.group(2))

        evaluation = ""
        css_class = "bw-neutral"
        
        # 履歴がある場合の比較
        if history:
            prev = history[0]
            prev_bw_str = prev.get('body_weight_str', '')
            prev_diff = 0
            prev_match = re.search(r'\(([\+\-]?\d+)\)', prev_bw_str)
            if prev_match:
                prev_diff = int(prev_match.group(1))
            
            prev_rank = prev.get('rank', 99)
            
            # 1. 絞れる (+評価)
            # 前走が大幅増(+8以上)で凡走(5着以下)し、今回マイナス(-4以下)
            if prev_diff >= 8 and prev_rank >= 5 and diff <= -4:
                evaluation = "絞れる(+)"
                css_class = "bw-positive"
            
            # 2. 回復 (+評価)
            # 前走、前々走と負のトレンドがあり、今回プラス
            elif len(history) >= 2:
                prev2 = history[1]
                prev2_bw_str = prev2.get('body_weight_str', '')
                prev2_diff = 0
                prev2_match = re.search(r'\(([\+\-]?\d+)\)', prev2_bw_str)
                if prev2_match:
                    prev2_diff = int(prev2_match.group(1))
                
                if prev_diff < 0 and prev2_diff < 0 and diff >= 4:
                    evaluation = "回復(+)"
                    css_class = "bw-positive"

            # 3. 余裕残し (-評価 / 注意)
            if not evaluation:
                if prev_diff >= 8 and prev_rank >= 5 and diff >= 0:
                    evaluation = "余裕残し(-)"
                    css_class = "bw-negative"
        
        # 4. 極端な増減の判定 (前走成績との比較)
        if not evaluation:
            if diff <= -14:
                # 激減だが、前走が大幅増で負けていれば「絞り込み成功」の可能性
                if history and prev_diff >= 10 and prev_rank >= 5:
                    evaluation = "究極仕上?"
                    css_class = "bw-positive"
                else:
                    evaluation = "反動or消耗?"
                    css_class = "bw-warning"
            elif diff >= 14:
                evaluation = "成長or成長?"
                if history and prev_rank <= 3:
                    evaluation = "太め残り!"
                    css_class = "bw-warning"
                else:
                    evaluation = "大幅増(!)"
                    css_class = "bw-warning"
            elif diff >= 10:
                evaluation = "太め残り?"
                css_class = "bw-negative"
            elif diff <= -10:
                # 前走好走しての-10kg以上は「消耗」の懸念
                if history and prev_rank <= 3:
                    evaluation = "消耗懸念?"
                    css_class = "bw-negative"
                else:
                    evaluation = "体調懸念?"
                    css_class = "bw-negative"

        display_text = f"{bw_info}"
        if evaluation:
            display_text += f" {evaluation}"
            
        return display_text, css_class

    def _generate_expert_comment(self, h_data, predicted_pace, omega_rank, pop_rank, all_horses_data=None, extra_info=None):
        """じゃい流・馬券名人分析コメントを生成します。
           期待値至上主義に基づき、オッズの歪み（バグ）を多角的に探索します。
        """
        score = 0
        comments = []
        
        # クッション値の考慮
        extra_info = extra_info or {}
        cushion_val = extra_info.get('cushion_value')
        if cushion_val:
            try:
                cv = float(cushion_val)
                # クッション値が9.0未満（柔らかい/タフ）な場合、重・不良実績のある馬を評価
                if cv < 9.0:
                    tc_rec = h_data.get('track_condition_record', '0-0-0-0')
                    # track_condition_record は "良 1-2-0-3" の形式なので、数字部分をパース
                    tc_rec_parts = tc_rec.split(' ')[-1].split('-')
                    if any(int(x) > 0 for x in tc_rec_parts[:3]): # 3着以内があれば
                        score += 2
                        comments.append(f"クッション値{cv}適合(タフ馬場◎)")
            except: pass
        
        # ============================================================
        # 1. 期待値判定: Ω値順位 vs 人気順位の乖離 (バグ度)
        # ============================================================
        if isinstance(omega_rank, int) and isinstance(pop_rank, int):
            gap = pop_rank - omega_rank  # プラス = 人気より実力上位 = 美味しい
            if gap >= 7:
                score += 3 # 8 -> 3 (抑制)
                comments.append(f"★回収期待値S(Ω{omega_rank}位vs{pop_rank}人気)")
            elif gap >= 5:
                score += 2 # 5 -> 2
                comments.append(f"★期待値高(Ω{omega_rank}位vs{pop_rank}人気)")
            elif gap >= 3:
                score += 1 # 3 -> 1
                comments.append(f"期待値有(Ω{omega_rank}位vs{pop_rank}人気)")
            elif gap <= -5:
                score -= 4 # -2 -> -4 (ペナルティ強化)
                comments.append("危:過剰評価(人気先行)")
            elif gap <= -3:
                score -= 2 # -1 -> -2
                comments.append("人気先行気味")

        # ============================================================
        # 2. 度外視評価: 前走敗因の質
        # ============================================================
        disregard = h_data.get('disregard', '')
        if disregard and disregard != '-':
            # 度外視できる理由があるほどプラス
            disregard_keywords = {
                '不良': 2, '重': 1, '馬場': 2,
                '展開': 2, 'ペース': 1,
                '距離': 2, '短縮': 1, '延長': 1,
                '不利': 3, '出遅れ': 2,
                '昇級': 2, '初': 1
            }
            found_reasons = []
            for kw, pts in disregard_keywords.items():
                if kw in disregard:
                    score += pts
                    found_reasons.append(kw)
            if found_reasons:
                comments.append(f"度外視可({','.join(found_reasons[:3])})")
        
        # ============================================================
        # 3. 展開適性: 予測ペース × 脚質
        # ============================================================
        style = h_data.get('running_style_main', h_data.get('running_style', ''))
        if predicted_pace == 'H':
            if style in ['差し', '追込']:
                score += 2
                comments.append("Hペース差し向き◎")
            elif style in ['逃げ']:
                score -= 4 # -2 -> -4
                comments.append("Hペース逃げ苦戦")
        elif predicted_pace == 'S':
            if style in ['逃げ', '先行']:
                score += 2
                comments.append("Sペース先行有利◎")
            elif style in ['追込']:
                score -= 4 # -2 -> -4
                comments.append("Sペース追込不利")

        # ============================================================
        # 4. 馬具バグ: ブリンカー(B)装着の変化
        # ============================================================
        blinker = h_data.get('blinker', '-')
        history = h_data.get('history', [])
        surface = h_data.get('surface', '芝') # 現在の馬場

        if blinker == '新規':
            # 芝初ブリンカー（過去にダートでB装着経験があり、今回芝で初B）
            has_past_dirt_blinker = False
            for r in history:
                if 'ダ' in r.get('surface', '') and r.get('blinker'):
                    has_past_dirt_blinker = True
                    break
            
            if surface == '芝' and has_past_dirt_blinker:
                score += 5
                comments.append("★★特大バグ:芝初ブリンカー(ダB経験あり)")
            else:
                score += 3
                comments.append("★B初装着→一変の可能性")
        elif blinker == '継続':
            # 装着2戦目の「慣れ」を評価
            b_count = 0
            for r in history:
                if r.get('blinker'): b_count += 1
                else: break
            if b_count == 1:
                score += 2
                comments.append("B装着2戦目(慣れ一変狙い)")
        elif blinker == '再':
            score += 2
            comments.append("B再装着→刺激期待")
        elif blinker == '外し':
            score += 1
            comments.append("B外し→素の走り期待")

        # 斤量変化の解析
        kinryo_diff = h_data.get('kinryo_diff', '±0')
        try:
            diff_val = float(kinryo_diff.replace('±', '').replace('+', ''))
        except:
            diff_val = 0
        
        # 馬体重評価（+10kg以上の増減をチェック）
        weight_info = h_data.get('weight_info', '')
        is_weight_heavy = False
        if weight_info and '(' in weight_info:
            import re
            w_match = re.search(r'\(([\+\-]?\d+)\)', weight_info)
            if w_match:
                w_diff = int(w_match.group(1))
                if w_diff >= 10: is_weight_heavy = True

        if diff_val <= -2:
            score += 2
            comments.append(f"斤量大幅減({kinryo_diff})→解放")
        elif diff_val <= -1:
            score += 1
            comments.append(f"斤量減({kinryo_diff})")
        
        # 酷量(58kg以上)チェック
        try:
            cur_kinryo = float(h_data.get('kinryo', 0))
            if cur_kinryo >= 58:
                if is_weight_heavy:
                    score -= 8 # -5 -> -8 (致命的減点)
                    comments.append(f"★消し:酷量{cur_kinryo}kg+大幅増量")
                else:
                    score -= 3 # -1 -> -3
                    comments.append(f"酷量{cur_kinryo}kg")
        except:
            pass

        # ============================================================
        # 6. コース・距離適性
        # ============================================================
        def parse_record(rec_str):
            """成績文字列 '1-2-0-3' をリストに変換"""
            if not rec_str or rec_str == '-': return [0,0,0,0]
            # 文字列の先頭部分から数字-数字-数字-数字を抽出
            import re
            m = re.match(r'(\d+)-(\d+)-(\d+)-(\d+)', rec_str)
            if m: return [int(m.group(i)) for i in range(1,5)]
            return [0,0,0,0]
        
        c_rec = parse_record(h_data.get('course_record', ''))
        d_rec = parse_record(h_data.get('distance_record', ''))
        
        # コース実績の勝率
        c_total = sum(c_rec)
        if c_total >= 3:
            c_win_rate = (c_rec[0] + c_rec[1] + c_rec[2]) / c_total
            if c_win_rate >= 0.5:
                score += 5 # 2 -> 5 (実績重視)
                comments.append(f"コース巧者({c_rec[0]}-{c_rec[1]}-{c_rec[2]}-{c_rec[3]})")
            elif c_win_rate >= 0.33:
                score += 2
                comments.append("コース適性有")
            elif c_win_rate == 0:
                score -= 1
                comments.append("コース実績なし")
        
        # 距離実績
        d_total = sum(d_rec)
        if d_total >= 3:
            d_win_rate = (d_rec[0] + d_rec[1] + d_rec[2]) / d_total
            if d_win_rate >= 0.5:
                score += 3 # 1 -> 3
                comments.append("距離適性優秀")
            elif d_win_rate >= 0.33:
                score += 1

        # ============================================================
        # 7. 同ペース同距離実績
        # ============================================================
        pc_rec = parse_record(h_data.get('pace_record', ''))
        pc_total = sum(pc_rec)
        if pc_total >= 2:
            pc_win_rate = (pc_rec[0] + pc_rec[1] + pc_rec[2]) / pc_total
            if pc_win_rate >= 0.5:
                score += 2
                comments.append(f"同ペース好走({pc_rec[0]}-{pc_rec[1]}-{pc_rec[2]}-{pc_rec[3]})")
            elif pc_win_rate == 0:
                score -= 1
                comments.append("同ペース実績なし")
        elif pc_total == 0:
            pass  # データなしは減点しない

        # ============================================================
        # 8. 間隔適性
        # ============================================================
        interval_rec = h_data.get('interval_rec', '-')
        if interval_rec and interval_rec != '-':
            i_rec = parse_record(interval_rec)
            i_total = sum(i_rec)
            if i_total >= 2:
                i_win_rate = (i_rec[0] + i_rec[1] + i_rec[2]) / i_total
                if i_win_rate >= 0.5:
                    score += 1
                    comments.append("間隔適性◎")
                elif i_win_rate == 0:
                    score -= 1
                    comments.append("間隔実績不振")

        # ============================================================
        # 9. 人気の断捨離: 人気馬の懸念材料チェック
        # ============================================================
        pop = h_data.get('popularity', 0)
        if pop and 1 <= pop <= 3:
            concerns = []
            if diff_val >= 1:
                concerns.append("斤量増")
            try:
                if cur_kinryo >= 58:
                    concerns.append("酷量")
            except:
                pass
            if blinker == '外し':
                concerns.append("B外し")
            # コース実績が悪い場合
            if c_total >= 2 and c_rec[0] == 0 and c_rec[1] == 0:
                concerns.append("コース未勝利")
            
            if len(concerns) >= 2:
                score -= 3
                comments.append(f"★消し候補({','.join(concerns)})")
            elif len(concerns) == 1:
                score -= 1
                comments.append(f"懸念あり({concerns[0]})")

        # ============================================================
        # 総合判定: 印の決定
        # ============================================================
        # 能力指数 (Ω等) による足切り: 下位30%の馬は期待値が高くても◎/○を避ける
        total_horses = len(all_horses_data) if all_horses_data else 10
        is_bottom_ability = isinstance(omega_rank, int) and omega_rank > (total_horses * 0.7)

        if score >= 7 and not is_bottom_ability:
            mark = '◎'
        elif score >= 4 and not is_bottom_ability:
            mark = '○'
        elif score >= 2:
            mark = '▲'
        elif score >= 0:
            mark = '△'
        else:
            # 1-3番人気の馬で、極端に悪い材料がない場合は足切りしない
            if 1 <= pop_rank <= 3 and score >= -2:
                mark = '△'
            else:
                mark = '無'
        
        # コメント構築
        if not comments:
            comment = '特筆事項なし'
        else:
            comment = ' / '.join(comments[:4])  # 最大4項目
        
        return mark, comment

    def _generate_performance_summary(self, h_data):
        """15項目以上の実績を統合評価し、最終判定と要約を生成します。"""
        score = 0
        pos = []
        neg = []

        # 1. 馬番・馬具 (B初装着などは加点)
        blinker = h_data.get('blinker', '-')
        if '初' in blinker or '再' in blinker:
            score += 2
            pos.append("馬具一変")

        # 2. 斤量増減
        k_diff = h_data.get('kinryo_diff', '')
        try:
            diff_val = float(k_diff.replace('+', '')) if k_diff else 0
            if diff_val <= -2:
                score += 2
                pos.append("大幅減量")
            elif diff_val >= 2:
                score -= 2
                neg.append("斤量増")
        except: pass

        # 3. 今回斤量成績
        k_rec_str = h_data.get('kinryo_record', '')
        if '初' not in k_rec_str and any(x in k_rec_str for x in ['1-', '2-', '3-']):
            score += 1
            pos.append("斤量適性")

        # 4. 間隔別
        i_rec = h_data.get('interval_rec', '')
        if any(x in i_rec for x in ['1-', '2-', '3-']):
            score += 1
        elif '0-0-0' in i_rec and i_rec != '0-0-0-0':
            score -= 1
            neg.append("間隔不安")

        # 5-8. コース/回り/距離/コース×距離
        def parse_rec(r):
            if not r: return 0, 0
            try:
                # 0-0-0-0 (その他) 形式のパース
                base = str(r).split(' ')[0]
                parts = list(map(int, base.split('-')))
                return sum(parts[:3]), sum(parts)
            except: return 0, 0

        c_top3, c_total = parse_rec(h_data.get('course_record', '0-0-0-0'))
        d_top3, d_total = parse_rec(h_data.get('distance_record', '0-0-0-0'))
        s_top3, s_total = parse_rec(h_data.get('surface_record', '0-0-0-0'))

        if s_top3 > 0:
            score += 3
            pos.append("条件ベスト")
        elif d_top3 > 0 or c_top3 > 0:
            score += 2
            pos.append("得意条件")

        # 9-10. 同条件最速/上り
        t_class = h_data.get('best_time_class', '')
        f_class = h_data.get('best_3f_class', '')
        if 'speedy' in t_class:
            score += 2
            pos.append("高速時計")
        if 'speedy' in f_class:
            score += 2
            pos.append("好上がり")

        # 11. 同ペース同距離
        p_top3, p_total = parse_rec(h_data.get('pace_record', '0-0-0-0'))
        if p_top3 > 0:
            score += 2
            pos.append("ペース適合")

        # 12-14. 枠/騎手/季節
        f_top3, f_total = parse_rec(h_data.get('frame_record', '0-0-0-0'))
        j_top3, j_total = parse_rec(h_data.get('jockey_record', '0-0-0-0'))
        sea_top3, sea_total = parse_rec(h_data.get('season_record', '0-0-0-0'))
        
        if f_top3 > 0: score += 1
        if j_top3 > 0: 
            score += 1
            if j_total > 0 and (j_top3 / j_total) >= 0.3:
                score += 1
                pos.append("相性優秀")
        if sea_top3 > 0: score += 1

        # 15. 脚質実績
        styl = h_data.get('style_display', '')
        if any(x in styl for x in ['1-', '2-', '3-']):
            score += 1
            pos.append("脚質適合")

        # 最終判定
        if score >= 10: judgment = "絶対買い"
        elif score >= 5: judgment = "買い"
        elif score >= 2: judgment = "見送り気味"
        else: judgment = "見送り"

        summary_text = " / ".join(pos[:3])
        if neg:
            summary_text += " (不安: " + ",".join(neg[:2]) + ")"
        
        if not summary_text: summary_text = "特筆すべき実績なし"

        return judgment, summary_text, score

    def get_full_analysis(self, race_id, extra_info=None):
        """レース全体の解析を行い、詳細なデータを返します。
           2パス処理に変更:
           1. 全馬のデータ取得と基本情報の収集 -> ペース予測
           2. 予測ペースに基づき、各馬の実績集計
        """
        horses = self.scrape_race_card(race_id)
        if not horses: return None
        
        race_info = self.scrape_race_info(race_id)
        popularity = self.scrape_odds(race_id)
        
        # 追加情報の取得
        extra_info = extra_info or {}
        cushion_val = extra_info.get('cushion_value')
        training_url = extra_info.get('training_url')
        
        cur_v = race_info['venue']
        cur_d = str(race_info['distance'])
        cur_s = race_info['surface']
        cur_rot = self.VENUE_ROTATION.get(cur_v, '右')
        
        cur_date_str = f"{race_id[:4]}/{race_id[4:6]}/{race_id[6:8]}"
        month = int(race_id[4:6]) if len(race_id) >= 6 else 1
        season_months = set(range(((month-1)//3)*3+1, ((month-1)//3)*3+4))
        
        fmt = lambda r: f"{r[0]}-{r[1]}-{r[2]}-{r[3]}"
        rec = lambda: [0, 0, 0, 0]
        
        # コース特性の取得
        course_chars = self.scrape_course_info(race_id)
        
        def normalize_str(s):
            if not s: return ""
            s = re.sub(r'\s+', '', str(s)).lower()
            s = re.sub(r'[▲△☆◇★]', '', s)
            return s
        
        # コース特性の名前を正規化
        fav_jockeys = [normalize_str(n) for n in course_chars.get('fav_jockeys', [])]
        fav_trainers = [normalize_str(n) for n in course_chars.get('fav_trainers', [])]
        fav_sires = [normalize_str(n) for n in course_chars.get('fav_sires', [])]
        fav_gates = course_chars.get('fav_gates', [])

        norm_cur_v = normalize_str(cur_v)
        try: cur_d_val = float(cur_d)
        except: cur_d_val = 0.0

        # ---------------------------------------------------------
        # Phase 1: データ収集と基本情報解析
        # ---------------------------------------------------------
        histories_cache = {}
        pre_data = [] # ペース予測用: {running_style, popularity, name}
        data_soup_cache = {}
        
        for h in horses:
            hid = h['horse_id']
            pop = popularity.get(h['number'], h.get('popularity', 0))
            h['popularity'] = pop # オッズ情報の反映
            
            data_soup = self.scrape_data_page(hid)
            data_soup_cache[hid] = data_soup
            history = self.scrape_horse_details(hid)
            histories_cache[hid] = history
            
            # 最新の脚質判定（直近3走の有効な通過順データから判定）
            valid_styles = []
            for r in history:
                if r['rank'] < 99 and r['running_style']:
                    valid_styles.append(r['running_style'])
                if len(valid_styles) >= 3: break
            
            main_style = '自在'
            if valid_styles:
                # 最も多い脚質を採用
                from collections import Counter
                counts = Counter(valid_styles)
                main_style = counts.most_common(1)[0][0]

            pre_data.append({
                'horse_id': hid,
                'name': h['horse_name'],
                'popularity': pop,
                'running_style': main_style
            })
            
        # ---------------------------------------------------------
        # Phase 2: ペース予測
        # ---------------------------------------------------------
        predicted_pace_data = self.predict_pace_from_list(pre_data)
        # 予測ペースを H/M/S に正規化してマッチングに使用
        pace_map = {'ハイ': 'H', 'ミドル': 'M', 'スロー': 'S', 'H': 'H', 'M': 'M', 'S': 'S'}
        predicted_pace = pace_map.get(predicted_pace_data['pace'], predicted_pace_data['pace'])
        
        # ---------------------------------------------------------
        # Phase 3: 詳細分析・集計
        # ---------------------------------------------------------
        all_data = []
        all_best_times = []
        all_best_3f = []
        
        for h in horses:
            hid = h['horse_id']
            pre = next((p for p in pre_data if p['horse_id'] == hid), {})
            history = histories_cache.get(hid, [])
            data_soup = data_soup_cache.get(hid)
            
            norm_cur_j = normalize_str(h['jockey'])
            cur_j_id = h.get('jockey_id', '')
            
            c_rec, r_rec, d_rec, s_rec, k_rec, j_rec, sea_rec, sty_rec, tc_rec = rec(), rec(), rec(), rec(), rec(), rec(), rec(), rec(), rec()
            frame_rec = [0, 0, 0, 0] # 枠実績
            pc_same_course_rec = rec() # 同ペース・同距離・同コース
            pc_diff_course_rec = rec() # 同ペース・同距離・他コース（着順カウント）
            pc_diff_course_list = [] # 同ペース・同距離・他コース (表示用リスト)
            
            best_t, best_t_detail = None, '-'
            ref_t, ref_t_detail = None, '-'
            best_3f, best_3f_detail = None, '-'
            ref_3f, ref_3f_detail = None, '-'
            
            last_bw, last_bw_s, last_k, last_rank = 0, '-', '-', 99
            last_run_found = False
            has_past_blinker = False
            sty_cnt = {'逃げ': 0, '先行': 0, '差し': 0, '追込': 0}

            # 枠順別実績
            stats_f = self.scrape_frame_stats(hid, soup=data_soup)
            cur_frame = str(h['frame'])
            if cur_frame in stats_f:
                try:
                    parts = list(map(int, stats_f[cur_frame].split('-')))
                    if len(parts) >= 4: frame_rec = parts[:4]
                except: pass
            
            # 間隔別実績
            interval_rec = '-'
            i_stats = self.scrape_interval_stats(hid, soup=data_soup)
            cur_interval = h.get('interval', '-')
            if cur_interval and cur_interval != '-' and i_stats:
                # 「中3週」等の数値を抽出
                intv_match = re.search(r'(\d+)', cur_interval)
                if intv_match:
                    iv = int(intv_match.group(1))
                    for k, v in i_stats.items():
                        nums = list(map(int, re.findall(r'(\d+)', k)))
                        if not nums: continue
                        
                        if len(nums) == 1:
                            if '以上' in k and iv >= nums[0]:
                                interval_rec = v
                                break
                            elif nums[0] == iv:
                                interval_rec = v
                                break
                        elif len(nums) == 2:
                            if nums[0] <= iv <= nums[1]:
                                interval_rec = v
                                break
                elif '連闘' in cur_interval and '連闘' in i_stats:
                    interval_rec = i_stats['連闘']
            
            # 斤量別成績 (履歴から計算するため初期化)
            k_rec = [0, 0, 0, 0]
            kinryo_record = '-'
            
            is_jockey_rode = False
            
            # 斤量差
            kinryo_diff = '-'
            prev_race = None
            for r in history:
                if r.get('date') != cur_date_str:
                    prev_race = r
                    break
            
            try:
                cur_k_val = float(h['kinryo'])
                if prev_race:
                    last_k_str = prev_race['kinryo']
                    match = re.search(r'(\d+\.?\d*)', str(last_k_str))
                    if match:
                        last_k_val = float(match.group(1))
                        diff = cur_k_val - last_k_val
                        kinryo_diff = f"{diff:+.1f}" if diff != 0 else "±0"
            except: pass

            for i, r in enumerate(history):
                # 今走自体のデータは統計に含めない
                if r.get('date') == cur_date_str: continue

                rank = r['rank']
                if rank >= 99: continue
                idx = 0 if rank == 1 else 1 if rank == 2 else 2 if rank == 3 else 3
                
                rv, rd, rs, rj, rk, rstyle, bw = r['venue'], str(r['distance']), r['surface'], r['jockey'], r['kinryo'], r['running_style'], r['body_weight']
                
                # 「前走」として情報を記録（今走除外後の最初のレース）
                if not last_run_found:
                    last_rank, last_bw, last_bw_s, last_k = rank, bw, r['body_weight_str'], rk
                    last_run_found = True
                
                if rstyle in sty_cnt: sty_cnt[rstyle] += 1
                
                # 過去のブリンカー使用履歴を確認
                if r.get('blinker'):
                    has_past_blinker = True

                # 騎手 (IDベースでの比較を優先、フォールバックで名前比較)
                rj = r.get('jockey', '')
                rj_id = r.get('jockey_id', '')
                is_match = False
                if rj_id and cur_j_id:
                    is_match = (rj_id == cur_j_id)
                else:
                    is_match = (normalize_str(rj) == norm_cur_j)

                if is_match:
                    j_rec[idx] += 1
                    is_jockey_rode = True
                
                # 斤量成績 (今走と一致する斤量の実績)
                try:
                    r_k_match = re.search(r'(\d+\.?\d*)', str(rk))
                    if r_k_match:
                        r_k_val = float(r_k_match.group(1))
                        if abs(r_k_val - cur_k_val) < 0.01: # 浮動小数点の誤差を考慮
                            k_rec[idx] += 1
                except: pass
                # 馬場状態 (良・稍・重・不)
                r_cond = r.get('condition', '')
                cur_tc = race_info.get('track_condition', '良')
                if cur_tc in r_cond:
                    tc_rec[idx] += 1
                
                # 各種条件
                try: rd_val = float(rd)
                except: rd_val = 0.0

                if normalize_str(rv) == norm_cur_v and rs == cur_s: c_rec[idx] += 1
                if normalize_str(rv) == norm_cur_v and rs == cur_s and abs(rd_val - cur_d_val) < 0.1:
                    s_rec[idx] += 1 # コース×距離実績
                
                try: r_rot = self.VENUE_ROTATION.get(rv, '')
                except: r_rot = ''
                if r_rot and cur_rot and r_rot[:1] == cur_rot[:1]: r_rec[idx] += 1
                
                if abs(rd_val - cur_d_val) < 0.1:
                    d_rec[idx] += 1
                    
                    # 同条件最速
                    t = r.get('time')
                    if t:
                        cond_char = '良' if '良' in r.get('condition', '') else '稍' if '稍' in r.get('condition', '') else '重' if '重' in r.get('condition', '') else '不' if '不' in r.get('condition', '') else '-'
                        detail = f"{rv}{int(rd_val)}{rs} {self._format_time(t)}({cond_char})"
                        
                        if normalize_str(rv) == norm_cur_v and rs == cur_s:
                            if best_t is None or t < best_t:
                                best_t = t
                                best_t_detail = detail
                        elif rs == cur_s:
                            # 競馬場不一致だが馬場・距離一致 (参考タイム候補)
                            if ref_t is None or t < ref_t:
                                ref_t = t
                                ref_t_detail = f"（参考）{detail}"

                    # 同ペース・同距離の判定
                    # 予測ペースかつ同一距離の場合
                    r_pace = r.get('pace', '')
                    # H/M/S と ハイ/ミドル/スロー の両方に対応
                    pace_map = {'H': 'H', 'M': 'M', 'S': 'S', 'ハイ': 'H', 'ミドル': 'M', 'スロー': 'S'}
                    norm_r_pace = pace_map.get(r_pace, r_pace)
                    norm_pred_pace = pace_map.get(predicted_pace, predicted_pace)
                    
                    if norm_r_pace and norm_r_pace == norm_pred_pace:
                        if normalize_str(rv) == norm_cur_v and rs == cur_s:
                            # 同ペース・同コース・同距離
                            pc_same_course_rec[idx] += 1
                        # コースに関わらず同ペース・同距離の実績もカウント
                        pc_diff_course_rec[idx] += 1
                        # 他コースでの3着以内は表示用リストに追加
                        if normalize_str(rv) != norm_cur_v and rank <= 3:
                            pc_diff_course_list.append(f"{rv}{rank}着")

                # 上がり3F
                try:
                    l3f = float(r.get('last_3f', 99.9))
                    if l3f < 99.0:
                        cond_char = '良' if '良' in r.get('condition', '') else '稍' if '稍' in r.get('condition', '') else '重' if '重' in r.get('condition', '') else '不' if '不' in r.get('condition', '') else '-'
                        detail_3f = f"{rv}{int(float(rd))}{rs} {l3f:.1f}({cond_char})"
                        
                        if normalize_str(rv) == norm_cur_v and abs(rd_val - cur_d_val) < 0.1 and rs == cur_s:
                            if best_3f is None or l3f < best_3f:
                                best_3f = l3f
                                best_3f_detail = detail_3f
                        elif abs(rd_val - cur_d_val) < 0.1 and rs == cur_s:
                            # 競馬場不一致だが距離・馬場一致 (参考上がり候補)
                            if ref_3f is None or l3f < ref_3f:
                                ref_3f = l3f
                                ref_3f_detail = f"（参考）{detail_3f}"
                except: pass
                
                # 季節
                try: 
                    if r.get('date'):
                        m = int(r['date'].split('/')[1])
                        if m in season_months: sea_rec[idx] += 1
                except: pass

            # 馬場状態成績のフォーマット (NEW: 「良 1-2-0-3」形式)
            cur_tc = race_info.get('track_condition', '良')
            condition_record = f"{cur_tc} {fmt(tc_rec)}"

            # 斤量成績のフォーマット
            cur_kinryo_label = f"{h['kinryo']}kg"
            if any(k_rec):
                kinryo_record = f"{cur_kinryo_label} {fmt(k_rec)}"
            else:
                kinryo_record = f"{cur_kinryo_label} 初斤量"

            # 馬具判定 (ブリンカー)
            blinker_display = '-'
            if h.get('blinker'):
                if prev_race and prev_race.get('blinker'):
                    blinker_display = '継続'
                elif has_past_blinker:
                    blinker_display = '再'
                else:
                    blinker_display = '新規'
            elif prev_race and prev_race.get('blinker'):
                blinker_display = '外し'

            # 騎手成績: 常に「騎手名 成績」形式で表示
            jockey_name = h['jockey']
            if is_jockey_rode:
                jockey_res_str = f"{jockey_name} {fmt(j_rec)}"
            else:
                jockey_res_str = f"{jockey_name}(初)"

            # 同条件上がりの最終決定 (同じ競馬場がない場合は参考を表示)
            if not best_3f and ref_3f:
                best_3f_detail = ref_3f_detail
            elif best_3f:
                all_best_3f.append((best_3f, hid))

            # 同条件最速の最終決定
            if not best_t and ref_t:
                best_t_detail = ref_t_detail
                # 参考タイムは色分け対象外にするため all_best_times には入れない
            elif best_t:
                all_best_times.append((best_t, hid))

            # ペース実績文字列の構築
            # 同ペース・同コース・同距離がある場合はそちらを優先表示
            if any(pc_same_course_rec):
                pc_rec_str = fmt(pc_same_course_rec)
                # 他コースの参考情報も追加
                if pc_diff_course_list:
                    others = ' '.join(pc_diff_course_list[:2])
                    if len(pc_diff_course_list) > 2: others += '...'
                    pc_rec_str += f" (他: {others})"
            elif any(pc_diff_course_rec):
                # 同コースなし → 同ペース・同距離（全コース）にフォールバック
                pc_rec_str = fmt(pc_diff_course_rec)
                if pc_diff_course_list:
                    others = ' '.join(pc_diff_course_list[:2])
                    if len(pc_diff_course_list) > 2: others += '...'
                    pc_rec_str += f" ({others})"
            else:
                pc_rec_str = fmt(pc_same_course_rec)  # 0-0-0-0

            # 脚質実績 (Phase 1 で判定した main_style の生涯成績を計算)
            style_main = pre.get('running_style', '自在')
            s_v = rec()
            for r in history:
                if r.get('running_style') == style_main and r['rank'] < 99:
                    idx2 = 0 if r['rank'] == 1 else 1 if r['rank'] == 2 else 2 if r['rank'] == 3 else 3
                    s_v[idx2] += 1
            
            style_rec_str = fmt(s_v)
            if style_main != '自在':
                style_display = f"{style_main} {style_rec_str}"
            else:
                style_display = f"自在 {style_rec_str}"
            
            # 度外視理由 (直近3走の敗因分析)
            # 引数を追加: cur_dist, cur_surface, cur_kinryo
            disregard, detailed_reasons = self._analyze_disregard_reasons(
                history, style_main, cur_date_str, cur_dist=cur_d, cur_surface=cur_s, cur_kinryo=h['kinryo']
            )

            # 買えるかどうか判定
            buyability, buyable_reason = self._evaluate_buyability(
                detailed_reasons, race_info.get('condition', '良'), cur_d, cur_s, predicted_pace, style_main
            )

            # Ω値の算出（最新5走の加重平均、類似条件での成績を評価）
            omega = h.get('omega', 0)
            if omega == 0 and history:
                omega_sum = 0
                weight_sum = 0
                for i, r in enumerate(history[:5]):
                    rank = r['rank']
                    if rank >= 99: continue
                    
                    # 着差（タイム差）を考慮したスコアリング
                    # 1着:100, 2着(僅差):95, 5着(僅差):70 などのように動的に算出
                    t_diff = r.get('time_diff', 0.0)
                    if rank == 1:
                        race_val = 100
                    else:
                        # 着差が小さいほど高評価 (着差0.0sなら100点、1.0sで40点、2.0s以上で0点という減衰モデル)
                        # 基礎点 = 100 - (タイム差 * 60) 
                        diff_score = max(0, 100 - (abs(t_diff) * 60))
                        # 着順による最低保障(または上限)を少し加味
                        rank_bonus = 80 if rank == 2 else 65 if rank == 3 else 50 if rank == 4 else 35 if rank == 5 else 10
                        race_val = (diff_score * 0.7) + (rank_bonus * 0.3)
                    
                    rv, rd, rs = r['venue'], str(r['distance']), r['surface']
                    
                    sim_weight = 1.0
                    if normalize_str(rv) == norm_cur_v: sim_weight += 0.5
                    if rd == cur_d: sim_weight += 0.5
                    if rs == cur_s: sim_weight += 0.3
                    try:
                        r_rot = self.VENUE_ROTATION.get(rv, '')
                        if r_rot and cur_rot and r_rot[:1] == cur_rot[:1]: sim_weight += 0.2
                    except: pass
                    
                    recency = max(0.5, 1.0 - (i * 0.1))
                    # クラス（格）による重み付け
                    r_class = r.get('race_class', '')
                    class_weight = self.GRADE_WEIGHTS.get(r_class, 4.0) / 4.0 # 1勝クラス(4.0)を基準(1.0)とする
                    
                    final_weight = sim_weight * recency * class_weight
                    
                    omega_sum += race_val * final_weight
                    weight_sum += final_weight
                
                omega = round(omega_sum / weight_sum, 1) if weight_sum > 0 else 0.0

            h_data = {
                'horse_id': hid,
                'name': h['horse_name'],
                'number': h['number'],
                'frame': h['frame'],
                'jockey': h['jockey'],
                'kinryo': h['kinryo'],
                'kinryo_diff': kinryo_diff,
                'popularity': h['popularity'],
                'omega': omega,
                'blinker': blinker_display, # 判定結果文字列
                'interval': h.get('interval', '-'),
                'interval_rec': interval_rec,
                'kinryo_record': kinryo_record,
                
                # コース要因 (新設)
                'is_fav_jockey': normalize_str(h['jockey']) in fav_jockeys,
                'is_fav_trainer': normalize_str(h.get('trainer', '')) in fav_trainers,
                'is_fav_sire': normalize_str(h.get('sire', '')) in fav_sires,
                'is_fav_gate': str(h['frame']) in fav_gates or str(h['number']) in fav_gates,
                
                'course_record': fmt(c_rec),
                'distance_record': fmt(d_rec),
                'surface_record': fmt(s_rec),
                'rotation_record': fmt(r_rec),
                'condition_record': condition_record,
                'track_condition_record': condition_record, # クッション値判定用に追加
                'season_record': fmt(sea_rec),
                'jockey_record': jockey_res_str,
                'frame_record': fmt(frame_rec),
                'pace_record': pc_rec_str, # フォーマット済み文字列
                'pc_record': pc_rec_str, # フロントエンドで使用されている別のキー名
                
                'best_time': best_t,
                'best_time_detail': best_t_detail,
                'best_3f': best_3f,
                'best_3f_detail': best_3f_detail,
                
                'disregard': disregard,
                'buyability': buyability,
                'buyable_reason': buyable_reason,
                
                'last_run': {
                    'rank': last_rank,
                    'bweight': last_bw,
                    'bweight_str': last_bw_s,
                    'kinryo': last_k
                },
                'running_style_main': style_main,
                'running_style': pre.get('running_style', '自在'),
                'style_display': style_display,  # 「脚質名 成績」形式
                'history': history,
                'url': h['horse_url'],
                'history_url': f"{self.BASE_URL}/db/horse/{hid}/"
            }
            
            # 馬体重評価の追加
            bw_info = h.get('body_weight_info', '-')
            bw_display, bw_class = self._analyze_weight_evaluation(bw_info, history)
            h_data['weight_info'] = bw_display
            h_data['weight_class'] = bw_class
            
            all_data.append(h_data)
        
        # ---------------------------------------------------------
        # Phase 4: ランク付け・色分け
        # ---------------------------------------------------------
        def rank_times(time_list):
            if not time_list: return {}
            sorted_t = sorted(time_list, key=lambda x: x[0])
            n = len(sorted_t)
            ranks = {}
            for idx, (t, hid) in enumerate(sorted_t):
                # ユーザー要望: 1位赤、2位黄。最下位グレー、ブービー水色。
                cls = ''
                if idx == 0:
                    cls = 'bg-red'
                elif n >= 2 and idx == n - 1:
                    cls = 'bg-gray'
                elif n >= 3 and idx == 1:
                    cls = 'bg-yellow'
                elif n >= 4 and idx == n - 2:
                    cls = 'bg-cyan'
                ranks[hid] = cls
            return ranks

        t_ranks = rank_times(all_best_times)
        f_ranks = rank_times(all_best_3f)
        
        # Ωランクと人気ランクの計算（馬券名人分析用）
        sorted_by_omega = sorted(all_data, key=lambda x: x.get('omega', 0), reverse=True)
        omega_ranks = {}
        for i, h in enumerate(sorted_by_omega):
            omega_ranks[h['horse_id']] = i + 1
        
        sorted_by_pop = sorted(all_data, key=lambda x: x.get('popularity', 999))
        pop_ranks = {}
        for i, h in enumerate(sorted_by_pop):
            pop_ranks[h['horse_id']] = i + 1
        
        for h_data in all_data: # Renamed 'h' to 'h_data' to avoid conflict with outer loop variable 'h'
            hid = h_data['horse_id']
            h_data['best_time_class'] = t_ranks.get(hid, '')
            h_data['best_3f_class'] = f_ranks.get(hid, '')
            
            # 馬券名人コメント生成
            omRank = omega_ranks.get(hid, '-')
            pRank = pop_ranks.get(hid, '-')
            
            # Web版詳細テーブル用
            h_data['omega_rank'] = omRank
            h_data['popularity_rank'] = pRank
            h_data['bug_degree'] = (pRank - omRank) if isinstance(omRank, int) and isinstance(pRank, int) else 0

            # Ωランク上位5位に暖色(オレンジ系)のクラスを付与
            h_data['omega_class'] = 'bg-warm-orange' if isinstance(omRank, int) and omRank <= 5 else ''
            # 人気ランク上位5位に暖色(ピーチ系)のクラスを付与
            h_data['pop_class'] = 'bg-warm-peach' if isinstance(pRank, int) and pRank <= 5 else ''

            # 最終スコアとコメント
            mark, comment = self._generate_expert_comment(h_data, predicted_pace, omRank, pRank, all_horses_data=all_data, extra_info=extra_info)
            h_data['expert_mark'] = mark
            h_data['expert_comment'] = comment
            
            # 実績サマリー
            judgment, summary, p_score = self._generate_performance_summary(h_data)
            h_data['performance_judgment'] = judgment
            h_data['performance_summary'] = summary
            h_data['total_score'] = p_score
            
            # 追加情報の反映（調教等）
            if training_url:
                h_data['expert_comment'] += f" / 調教注目(外部URL参照)"
            
            # コース要因による上書き調整
            if h_data.get('is_fav_jockey') or h_data.get('is_fav_trainer') or h_data.get('is_fav_sire') or h_data.get('is_fav_gate'):
                h_data['total_score'] += 2
                fav_parts = []
                if h_data.get('is_fav_jockey'): fav_parts.append("買い手(騎手)")
                if h_data.get('is_fav_trainer'): fav_parts.append("買い手(厩舎)")
                if h_data.get('is_fav_sire'): fav_parts.append("買い手(血統)")
                if h_data.get('is_fav_gate'): fav_parts.append("買い枠(" + ",".join(fav_gates) + ")")
                
                if h_data['performance_judgment'] == "見送り": h_data['performance_judgment'] = "検討"
                h_data['performance_summary'] = "★コース相性◎(" + ",".join(fav_parts) + ") " + h_data['performance_summary']
            
        # ---------------------------------------------------------
        # Phase 5: 軸馬候補（有力馬3頭）の選定 (ハイブリッド方式)
        # ---------------------------------------------------------
        # 動的重み付け（Optimizer）の読み込み (ループ外へ移動)
        optimized_params = {}
        params_path = os.path.join(os.path.dirname(__file__), "optimized_params.json")
        if os.path.exists(params_path):
            try:
                with open(params_path, 'r', encoding='utf-8') as f:
                    optimized_params = json.load(f)
            except: pass

        # 学習型フィードバックデータの読み込み
        feedback_data = {"patterns": []}
        feedback_path = os.path.join(os.path.dirname(__file__), "feedback_data.json")
        if os.path.exists(feedback_path):
            try:
                with open(feedback_path, 'r', encoding='utf-8') as f:
                    feedback_data = json.load(f)
            except: pass

        # 記号の強さ定義
        mark_weight = {'◎': 100, '○': 80, '▲': 60, '△': 40, '無': 0}
        
        # 1. ◎ 選定: 総合スコア最高 (ハイブリッド軸)
        best_hybrid = max(all_data, key=lambda x: self.get_total_score_with_reasons(x, race_info, feedback_data, mark_weight, optimized_params)[0])
        
        # 2. ○ 選定: ◎と異なる脚質の中でスコア最高 (能力・展開分散軸)
        remaining_for_ability = [h for h in all_data if h['horse_id'] != best_hybrid['horse_id']]
        best_hybrid_style = best_hybrid.get('running_style_main', '')
        diff_style_horses = [h for h in remaining_for_ability if h.get('running_style_main', '') != best_hybrid_style]
        if diff_style_horses:
            best_ability = max(diff_style_horses, key=lambda x: self.get_total_score_with_reasons(x, race_info, feedback_data, mark_weight, optimized_params)[0])
        else:
            best_ability = max(remaining_for_ability, key=lambda x: self.get_total_score_with_reasons(x, race_info, feedback_data, mark_weight, optimized_params)[0]) if remaining_for_ability else None
        
        # 3. ▲ 選定: 6番人気以下の穴馬でスコア最高 (期待値・高配当軸)
        excluded_ids = {best_hybrid['horse_id']}
        if best_ability: excluded_ids.add(best_ability['horse_id'])
        remaining_for_expert = [h for h in all_data if h['horse_id'] not in excluded_ids]
        underdog_horses = [h for h in remaining_for_expert if h.get('popularity', 99) >= 6]
        if underdog_horses:
            best_expert = max(underdog_horses, key=lambda x: self.get_total_score_with_reasons(x, race_info, feedback_data, mark_weight, optimized_params)[0])
        else:
            best_expert = max(remaining_for_expert, key=lambda x: self.get_total_score_with_reasons(x, race_info, feedback_data, mark_weight, optimized_params)[0]) if remaining_for_expert else None

        raw_recommends = [best_hybrid]
        if best_ability: raw_recommends.append(best_ability)
        if best_expert: raw_recommends.append(best_expert)

        recommendations = []
        for i, h in enumerate(raw_recommends):
            # 詳細な理由の構成：実績サマリーとAIによる具体的理由を結合
            score, auto_reasons = self.get_total_score_with_reasons(h, race_info, feedback_data, mark_weight, optimized_params)
            rec_reason = h['performance_summary']
            label = "【総合能力軸】" if i == 0 else "【展開分散軸】" if i == 1 else "【注目穴馬軸】"
            
            # 詳細な理由リストを結合
            detail_text = " / ".join(auto_reasons) if auto_reasons else "総合的な条件適性"
            full_reason = f"{label} {detail_text} ({rec_reason})"
            
            recommendations.append({
                'number': h['number'],
                'name': h['name'],
                'mark': h['expert_mark'],
                'omega': h.get('omega', 0),
                'judgment': h['performance_judgment'],
                'reason': full_reason,
                'score': score
            })

        # --- 信頼度（期待値/ROIスコア）の計算 ---
        confidence = 0
        if raw_recommends:
            top_h = raw_recommends[0]
            top_score, top_reasons = self.get_total_score_with_reasons(top_h, race_info, feedback_data, mark_weight, optimized_params)
            sorted_all = sorted(all_data, key=lambda x: self.get_total_score_with_reasons(x, race_info, feedback_data, mark_weight, optimized_params)[0], reverse=True)
            
            # 基礎点: 上位3頭の平均スコアをベースに調整 (上限を撤廃)
            avg_top3_score = sum(self.get_total_score_with_reasons(h, race_info, feedback_data, mark_weight, optimized_params)[0] for h in raw_recommends) / len(raw_recommends)
            # スケールを 0.8 に固定し、上限を撤廃 (下限のみ30点)
            base_score = max(avg_top3_score * 0.8, 30) 
            
            # 1. ロジック自信度（絶対的なスコアの高さ）
            if top_score >= 90: base_score += 15 
            elif top_score >= 80: base_score += 5 
                
            # 2. 期待値ボーナス (人気との乖離)
            try:
                pop_rank = int(top_h.get('popularity_rank', 99))
                if pop_rank > 1:
                    # 最大加点制限をなくし、より穴馬の期待値を反映
                    bonus = (pop_rank - 1) * 2
                    base_score += bonus
            except: pass
            
            # 3. 軸馬(1位)とライバル(2位)の差（優位性）
            if len(sorted_all) >= 2:
                diff_1_2 = top_score - self.get_total_score_with_reasons(sorted_all[1], race_info, feedback_data, mark_weight, optimized_params)[0]
                if diff_1_2 >= 30: base_score += 20 # 優位性が顕著な場合の加点を強化
                elif diff_1_2 >= 20: base_score += 5 
                elif diff_1_2 < 12: base_score -= 20 
            
            # 4. 混戦・波乱度ペナルティ
            if len(sorted_all) >= 5:
                diff_1_4 = top_score - self.get_total_score_with_reasons(sorted_all[3], race_info, feedback_data, mark_weight, optimized_params)[0]
                if diff_1_4 < 25: base_score -= 25 
            
            # 5. 頭数・クラス補正
            h_count = len(all_data)
            if h_count <= 10: base_score += 3 
            elif h_count >= 16: base_score -= 15 
            
            r_class = race_info.get('class', '') or race_info.get('class_name', '')
            if any(g in r_class for g in ['G1', 'G2', 'G3', '重賞']):
                base_score += 5 
            elif '未勝利' in r_class or '新馬' in r_class:
                base_score -= 15 
                
            confidence = max(0, int(base_score)) # 上限100の制限を撤廃

        # ヒモ候補（3着候補5頭）の選定
        sorted_others = sorted([h for h in all_data if h['horse_id'] not in {h['horse_id'] for h in raw_recommends}], 
                                key=lambda x: self.get_total_score_with_reasons(x, race_info, feedback_data, mark_weight, optimized_params)[0], reverse=True)
        
        raw_himo = sorted_others[:5]
        # 【大穴枠強制確保】8番人気以下の馬が含まれているかチェック
        has_oana = any(h.get('popularity', 99) >= 8 for h in raw_himo)
        if not has_oana and len(sorted_others) > 5:
            oana_candidates = [h for h in sorted_others[5:] if h.get('popularity', 99) >= 8]
            if oana_candidates:
                raw_himo[4] = oana_candidates[0]

        himo_horses = []
        for h in raw_himo:
            score, auto_reasons = self.get_total_score_with_reasons(h, race_info, feedback_data, mark_weight, optimized_params)
            # 軸馬と同様に AI理由 + 実績サマリー を結合
            summary = h.get('performance_summary', '')
            detail_text = " / ".join(auto_reasons) if auto_reasons else ""
            if detail_text and summary:
                full_reason = f"{detail_text} ({summary})"
            else:
                full_reason = detail_text or summary or "条件見合いで浮上"

            himo_horses.append({
                'number': h['number'],
                'name': h['name'],
                'mark': h['expert_mark'],
                'judgment': h['performance_judgment'],
                'reason': full_reason,
                'score': score
            })

        # --- 消し馬（激走率低減のための厳格選定） ---
        rec_ids = {h['number'] for h in recommendations}
        himo_ids = {h['number'] for h in himo_horses}
        excluded_ids_nums = rec_ids | himo_ids
        
        other_horses = []
        for h in all_data:
            if h['number'] not in excluded_ids_nums:
                score, auto_reasons = self.get_total_score_with_reasons(h, race_info, feedback_data, mark_weight, optimized_params)
                pop = h.get('popularity', 99)
                p_score = h.get('total_score', 0)

                # 消し馬判定の徹底緩和：実績スコアの上限を引き上げ、より多くの馬を候補に入れる
                if p_score >= 3.5: continue 
                if h.get('expert_mark', '無') != '無': continue
                
                last_run = h.get('last_run', {})
                last_rank = last_run.get('rank')
                try: last_rank = int(last_rank) if last_rank is not None else 99
                except: last_rank = 99

                # 走数チェック：初出走（新馬）はデータ不足のため「消し」にはしない
                total_runs = 0
                try:
                    all_rec = h.get('total_record', '0-0-0-0')
                    total_runs = sum(map(int, re.findall(r'\d+', all_rec.split(' ')[0])))
                except: pass

                if total_runs == 0: continue # 新馬・未出走は除外

                # 前走3着以内は激走リスクがあるため消さない
                if last_rank <= 3: continue 
                
                # 芝/ダート適性の整合性チェック
                cur_surface = race_info.get('surface', '')
                last_surface = last_run.get('surface', '')
                surface_mismatch = False
                if cur_surface and last_surface and cur_surface != last_surface:
                    s_rec = h.get('surface_record', '0-0-0-0')
                    if s_rec.startswith('0-0-'): surface_mismatch = True

                def has_top2_record(record_str):
                    if not record_str or record_str == '0-0-0-0': return False
                    try:
                        base = str(record_str).split(' ')[0]
                        parts = [int(p) for p in re.findall(r'\d+', base)]
                        return any(p > 0 for p in parts[:2])
                    except: return False

                # ネガティブな理由のみを抽出するフィルタ
                exclude_reasons = []
                if surface_mismatch:
                    exclude_reasons.append(f"今回の{cur_surface}実績に乏しく適性不安")
                
                if last_rank >= 10:
                    exclude_reasons.append(f"前走{last_rank}着と大敗")
                elif 4 <= last_rank <= 9:
                    exclude_reasons.append("近走の精彩を欠く")
                
                if not has_top2_record(h.get('course_record')) and not has_top2_record(h.get('distance_record')):
                    exclude_reasons.append("コース・距離実績に乏しい")
                
                # AIによる理由(auto_reasons)から、減点項目(マイナス表記)のみを抽出
                negative_ai = []
                for r in auto_reasons:
                    if '(-' in r:
                        # (-5) などの数値を消して理由として整形
                        clean_r = re.sub(r'\(-?\d+\)', '', r).strip()
                        negative_ai.append(f"{clean_r}等の不安")
                
                exclude_reasons.extend(negative_ai[:2])

                # 理由がなくてもスコアが極端に低い場合は機械的に抽出対象とする
                if not exclude_reasons:
                    if p_score < 1.0:
                        exclude_reasons.append("強調材料に乏しい")
                    elif p_score < 2.5:
                        exclude_reasons.append("目立った実績がなく静観妥当")
                    else:
                        continue

                other_horses.append({
                    'number': h['number'],
                    'name': h['name'],
                    'reason': " / ".join(exclude_reasons),
                    'score': score,
                    'p_score': p_score
                })
        
        # 不安要素が多い（期待値scoreが低い ＆ 実績スコアp_scoreが低い）順にソート
        other_horses.sort(key=lambda x: (x['score'], x['p_score']))
        # 確実に3頭選出（候補が3頭未満なら、除外された馬の中からスコアが低い順に補致する）
        discouraged = other_horses[:3]
        if len(discouraged) < 3 and len(all_data) > (len(recommendations) + len(himo_horses)):
            # まだ枠がある場合は、未選定の馬からスコア順に追加
            already_selected = {h['number'] for h in recommendations} | {h['number'] for h in himo_horses} | {h['number'] for h in discouraged}
            remaining = sorted([h for h in all_data if h['number'] not in already_selected], 
                               key=lambda x: self.get_total_score_with_reasons(x, race_info, feedback_data, mark_weight, optimized_params)[0])
            for h in remaining:
                if len(discouraged) >= 3: break
                score, auto_reasons = self.get_total_score_with_reasons(h, race_info, feedback_data, mark_weight, optimized_params)
                discouraged.append({
                    'number': h['number'],
                    'name': h['name'],
                    'reason': "強豪揃いで相対的な優位性乏しい" if not auto_reasons else " / ".join([re.sub(r'\(.*?\)', '', r) for r in auto_reasons if '(-' in r][:2]) or "実績条件面で強調材料なし",
                    'score': score,
                    'p_score': h.get('total_score', 0)
                })

        # ---------------------------------------------------------
        # Phase 6: 具体的買い目戦略
        # ---------------------------------------------------------
        betting_strategies = []
        if recommendations:
            top1 = recommendations[0]
            
            partners = []
            if len(recommendations) > 1:
                partners.extend([h['number'] for h in recommendations[1:]])
            if himo_horses:
                partners.extend([h['number'] for h in himo_horses])
            
            partners_str = ",".join(map(str, partners))
            
            if top1 and partners:
                umaren_msg = f"◎{top1['number']}から{partners_str}への馬連流し({len(partners)}点)。本命が穴なら複勝も推奨。"
                betting_strategies.append({"type": "馬連流し", "content": umaren_msg})
            
                sanrenpuku_msg = f"◎{top1['number']}を1頭目に、{partners_str}への三連複流し。期待回収率スコア: {confidence}"
                betting_strategies.append({"type": "三連複/ワイド", "content": sanrenpuku_msg})

        # Web版互換性のためのデータ整形
        for h in all_data:
            h['mark_text'] = h.get('expert_mark', '無')
            h['reason'] = h.get('performance_summary', '')

        # 買い目戦略を文字列にまとめる
        strategy_text = ""
        for s in betting_strategies:
            strategy_text += f"【{s['type']}】\n{s['content']}\n\n"

        return {
            'race_info': {**race_info, **predicted_pace_data},
            'pace_analysis': predicted_pace_data.get('pace_comment', '展開データなし'),
            'confidence': confidence,
            'horses': all_data,
            'full_results': all_data, # Web版用エイリアス
            'recommendations': recommendations,
            'himo_horses': himo_horses,
            'discouraged': discouraged,
            'betting_strategies': betting_strategies,
            'strategy': strategy_text.strip() # Web版用
        }
    def get_total_score_with_reasons(self, h_item, race_info, feedback_data, mark_weight, optimized_params=None):
        reasons = []
        raw_m_score = mark_weight.get(h_item.get('expert_mark', '無'), 0)
        # 専門家評価の圧縮を緩和（0.5 -> 0.7）
        m_score = raw_m_score * 0.7
        if raw_m_score >= 60:
            reasons.append(f"予想印{h_item['expert_mark']}高評価(総合的な条件適性)")
            
        p_score = h_item.get('total_score', 0)
        if p_score >= 5:
            reasons.append("実績スコア優秀")
        
        adj = 0
        pop = h_item.get('popularity', 99)
        
        # --- 消し馬判定の緩和: 人気馬のマイナスを抑える ---
        if pop <= 5: # 4 -> 5
            c_rec_str = h_item.get('course_record', '0-0-0-0')
            try:
                c_base = str(c_rec_str).split(' ')[0]
                c_parts = list(map(int, c_base.split('-')))
                c_total = sum(c_parts)
                if c_total >= 3 and (c_parts[0] + c_parts[1]) == 0:
                    adj -= 3 # -1 -> -3 (人気馬のコース実績なしは厳しく)
                    reasons.append("人気馬でコース連対なし(-3)")
            except: pass
            
            last_rank = h_item.get('last_run', {}).get('rank', 0)
            k_diff_str = h_item.get('kinryo_diff', '')
            try:
                k_diff_val = float(k_diff_str.replace('+', '')) if k_diff_str else 0
                if isinstance(last_rank, (int, float)) and last_rank > 10 and k_diff_val >= 1.5:
                    adj -= 3 # -1 -> -3
                    reasons.append(f"前走凡走+斤量増(-3)")
            except: pass
        
        # --- クラス（格）による補正 ---
        cur_class = race_info.get('class_name', '')
        cur_class_weight = self.GRADE_WEIGHTS.get(cur_class, 4.0)
        
        # 過去に今回と同等以上のクラスで掲示板(5着以内)の実績があるか
        history = h_item.get('history', [])
        high_class_match = False
        for r in history[:10]:
            r_class = r.get('race_class', '')
            r_rank = r.get('rank', 99)
            if self.GRADE_WEIGHTS.get(r_class, 0) >= cur_class_weight and r_rank <= 5:
                high_class_match = True
                break
        
        if high_class_match:
            adj += 5
            reasons.append(f"同クラス以上実績あり(+5)")
        else:
            # 格上挑戦（過去にこのクラスでの掲示板実績なし）
            if cur_class_weight >= 6.0: # 3勝クラス以上
                adj -= 3
                reasons.append(f"上位クラス初挑戦・実績不足(-3)")
        
        # --- 芝/ダート適性の整合性チェック（予想ロジックへの反映） ---
        cur_surface = race_info.get('surface', '')
        last_run = h_item.get('last_run', {})
        last_surface = last_run.get('surface', '')
        if cur_surface and last_surface and cur_surface != last_surface:
            # 前走と馬場が違う場合、今回の馬場での連対実績があるかチェック
            s_rec = h_item.get('surface_record', '0-0-0-0')
            if s_rec.startswith('0-0-'):
                # 過去に今回の馬場で連対がないなら大幅減点
                adj -= 8
                reasons.append(f"別馬場激走(今回の{cur_surface}適性不安)(-8)")
            else:
                # 連対実績はあっても前走の勢いが今回の馬場で通用するかは疑問符
                adj -= 3
                reasons.append(f"前走と馬場条件が異なる(-3)")
        
        # --- 実績重視の加点強化 ---
        # コース・距離の実績がある穴馬をより強く拾う
        if 4 <= pop <= 12:
            c_rec_str = h_item.get('course_record', '0-0-0-0')
            try:
                c_base = str(c_rec_str).split(' ')[0]
                c_parts = list(map(int, c_base.split('-')))
                c_top3 = sum(c_parts[:3])
                c_total = sum(c_parts)
                if c_total >= 2 and c_top3 / c_total >= 0.4:
                    adj += 10 # 8 -> 10
                    reasons.append(f"穴馬だがコース適性高({c_parts[0]}-{c_parts[1]}-{c_parts[2]}-{c_parts[3]})(+10)")
            except: pass

        # --- 【大穴激走注意】ロジック ---
        if pop >= 8:
            is_oana_hit = False
            try:
                c_rec_str = h_item.get('course_record', '0-0-0-0')
                c_base = str(c_rec_str).split(' ')[0]
                c_parts = list(map(int, c_base.split('-')))
                if sum(c_parts) >= 2 and (c_parts[0]+c_parts[1]+c_parts[2])/sum(c_parts) >= 0.33:
                    is_oana_hit = True
                
                d_rec_str = h_item.get('distance_record', '0-0-0-0')
                d_base = str(d_rec_str).split(' ')[0]
                d_parts = list(map(int, d_base.split('-')))
                if sum(d_parts) >= 2 and (d_parts[0]+d_parts[1]+d_parts[2])/sum(d_parts) >= 0.33:
                    is_oana_hit = True
            except: pass
            
            if is_oana_hit:
                adj += 12
                reasons.append("【大穴激走注意】条件合致(+12)")

        # パラメータ最適化の反映
        if optimized_params:
            jid = h_item.get('jockey_id', '')
            if jid in optimized_params.get('jockey_weights', {}):
                bonus = optimized_params['jockey_weights'][jid]
                adj += bonus
                if bonus > 0: reasons.append(f"騎手コース相性良好(+{bonus})")
                elif bonus < -2: reasons.append(f"騎手コース実績不安({bonus})")
            
            style = h_item.get('running_style_main', '')
            surface = race_info.get('surface', '')
            style_key = f"{surface}_{style}"
            if style_key in optimized_params.get('style_weights', {}):
                weight = optimized_params['style_weights'][style_key]
                bonus = (weight - 1.0) * 10
                adj += bonus
                if weight > 1.1: reasons.append(f"脚質({style})有利(+{round(bonus,1)})")
                elif weight < 0.9: reasons.append(f"脚質({style})不利({round(bonus,1)})")

        # 最終スコア算出（実績スコア p_score の重みを強化）
        total = m_score + (p_score * 3.0) + adj
        return total, reasons

    def _format_time(self, seconds):
        if not seconds: return '-'
        m = int(seconds // 60)
        s = seconds % 60
        return f"{m}:{s:04.1f}"
