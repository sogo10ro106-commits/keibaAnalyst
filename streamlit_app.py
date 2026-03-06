import streamlit as st
import pandas as pd
from scraper import KeibaLabScraper
import datetime
import json
import re

# ページ設定
st.set_page_config(
    page_title="競馬分析ツール (v260306_1005)",
    page_icon="🏇",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# スタイル設定（ダークモード風）
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
    }
    .stButton>button {
        width: 100%;
        border-radius: 8px;
    }
    .status-badge {
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.8rem;
        font-weight: bold;
    }
    .omega-high { color: #f1c40f; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# セッション状態の初期化
if 'scraper' not in st.session_state:
    st.session_state.scraper = KeibaLabScraper()
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = {}
if 'user_marks' not in st.session_state:
    st.session_state.user_marks = {} # race_id -> {horse_number: mark}

def get_mark(rank):
    marks = {1: "◎", 2: "○", 3: "▲", 4: "△", 5: "☆", 6: "×"}
    return marks.get(rank, "")

# --- メインエリアの構成 ---
st.title("🏇 競馬分析ツール")

# 1. 日付選択とシステム管理 (最上部に配置)
m_col1, m_col2 = st.columns([1, 2])
with m_col1:
    selected_date = st.date_input("📅 開催日選択", datetime.date.today())
    date_str = selected_date.strftime("%Y%m%d")

with m_col2:
    cc_col1, cc_col2 = st.columns([1, 1])
    with cc_col1:
        if st.button("🔄 キャッシュをクリア", use_container_width=True, type="secondary"):
            st.session_state.races_cache = {}
            st.session_state.analysis_results = {}
            st.success("クリアしました")
            st.rerun()
    with cc_col2:
        with st.expander("🛠️ システム管理"):
            if st.session_state.scraper.is_protected:
                if st.button("🔓 保護リセット", type="primary", use_container_width=True):
                    st.session_state.scraper.is_protected = False
                    st.session_state.scraper.consecutive_errors = 0
                    st.rerun()
            else:
                st.success("✅ 通信正常")
            st.caption(f"連続エラー: {st.session_state.scraper.consecutive_errors}")

st.divider()

# 2. レース一覧取得 (キャッシュ利用)
if 'races_cache' not in st.session_state:
    st.session_state.races_cache = {}

if date_str not in st.session_state.races_cache or not st.session_state.races_cache[date_str]:
    with st.spinner("レース一覧を取得中..."):
        try:
            res = st.session_state.scraper.get_races_by_date(date_str, skip_scoring=True)
            r = res.get('races', [])
            if r:
                st.session_state.races_cache[date_str] = r
            else:
                st.session_state.races_cache.pop(date_str, None)
        except Exception as e:
            st.error(f"エラーが発生しました: {e}")

races = st.session_state.races_cache.get(date_str, [])

if not races:
    if st.session_state.scraper.is_protected:
        st.error("保護モードが作動中のため、データの取得を制限しています。「保護リセット」を試してください。")
    else:
        st.info("該当日のレースが見つかりません。JRAの開催日（土日など）を選択しているかご確認ください。")
        st.warning("⚠️ 解決しない場合は、上の「🔄 キャッシュをクリア」を押してから、再度日付を選び直してください。")
else:
    st.caption(f"✅ {len(races)} 件のレースを表示中")
    
    # 3. 会場絞り込みとレース選択
    venues = sorted(list(set(r.get('venue') for r in races if r.get('venue'))))
    
    s_col1, s_col2 = st.columns([1, 2])
    with s_col1:
        selected_venue = st.selectbox("📍 会場絞り込み", ["すべて"] + venues)
    
    display_races = races
    if selected_venue != "すべて":
        display_races = [r for r in races if r.get('venue') == selected_venue]

    with s_col2:
        # レース選択 (名前が重複しないように ID を付与)
        race_options = {f"{r['name']}": r for r in display_races}
        selected_race_label = st.selectbox("🏇 分析するレースを選択してください", list(race_options.keys()))
    
    if selected_race_label:
        race = race_options[selected_race_label]
        race_id = race['id']
        
        # セッションから解析結果を事前取得 (リンク表示用)
        analysis = st.session_state.analysis_results.get(race_id, {})
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("🏁 レース情報")
            race_url = f"https://www.keibalab.jp/db/race/{race_id}/"
            st.markdown(f'**<a href="{race_url}" target="_blank">{race["name"]} 🔗</a>**', unsafe_allow_html=True)
            st.write(f"🕒 {race.get('time', '--:--')} 発走")
            st.write(f"📏 {race.get('surface', '')} {race.get('distance', '')}")
            st.write(f"🐎 {race.get('horse_count', 0)}頭立て / {race.get('weight_cond', '定量')}")

            # 追加情報の入力
            with st.expander("⚙️ 追加オプション"):
                cushion = st.number_input("クッション値", min_value=0.0, max_value=12.0, value=9.2, step=0.1)
                st.info("※9.0未満でタフ馬場適性馬が評価されます。")
                
            analyze_btn = st.button("🚀 分析開始 (Deep Analysis)", type="primary")

        if analyze_btn:
            with st.spinner("解析中... (30〜60秒程度かかります)"):
                try:
                    extra_info = {"cushion_value": cushion}
                    analysis = st.session_state.scraper.get_full_analysis(race_id, extra_info=extra_info)
                    st.session_state.analysis_results[race_id] = analysis
                except Exception as e:
                    st.error(f"解析エラー: {e}")

        # 解析結果の表示
        if race_id in st.session_state.analysis_results:
            analysis = st.session_state.analysis_results[race_id]
            
            st.divider()
            
            # --- 軸馬 (Axis Horses) ---
            st.markdown("### 🎯 軸馬候補 (上位3頭)")
            recs = analysis.get('recommendations', [])
            if recs:
                cols_axis = st.columns(3)
                for i, horse in enumerate(recs[:3]):
                    with cols_axis[i]:
                        mark = get_mark(i+1)
                        # スタイル調整用のクラス（簡易表現）
                        color = "#e74c3c" if i == 0 else "#f39c12" if i == 1 else "#3498db"
                        st.markdown(f"""
                        <div style="background: rgba(255,255,255,0.05); padding: 15px; border-radius: 10px; border-left: 5px solid {color}; box-shadow: 0 4px 10px rgba(0,0,0,0.3);">
                            <h3 style="margin:0; color:{color}; font-size: 1.3rem;">{mark} {horse['number']}. {horse['name']}</h3>
                            <p style="margin:5px 0; font-size:1.1rem; font-weight:bold;">{horse['judgment']}</p>
                            <p style="margin:2px 0; font-size:1.0rem; color:#ffd700; font-weight:bold;">期待値: {horse.get('score', 0):.1f}</p>
                            <p style="margin:0; font-size:0.85rem; color:#aaa;">Ω指数: <span style="color:#fff; font-weight:bold;">{horse['omega']}</span></p>
                        </div>
                        """, unsafe_allow_html=True)
                        st.caption(f"理由: {horse.get('reason', '')}")

            # --- ヒモ馬 (Himo Horses) ---
            st.subheader("🔗 ヒモ馬候補 (Himo Horses)")
            himo = analysis.get('himo_horses', [])
            if himo:
                himo_cols = st.columns(len(himo))
                for i, h in enumerate(himo):
                    with himo_cols[i]:
                        st.markdown(f"**{h['number']} {h['name']}**")
                        st.markdown(f"期待値スコア: {h.get('score', 0):.1f}")
                        st.write(h['reason'])

            # --- 消し馬 (Eliminated Horses) ---
            st.subheader("❌ 消し馬 (Discouraged Horses)")
            discouraged = analysis.get('discouraged', [])
            if discouraged:
                disc_cols = st.columns(len(discouraged))
                for i, h in enumerate(discouraged):
                    with disc_cols[i]:
                        st.markdown(f"**{h['number']} {h['name']}**")
                        st.markdown(f"期待値スコア: {h.get('score', 0):.1f}")
                        st.error(h['reason'])

            # --- 詳細分析テーブル (Detailed Table) ---
            st.divider()
            st.markdown("### 🏇 詳細分析データ (全項目)")
            all_horses = analysis.get('full_results', [])
            if all_horses:
                df_data = []
                # セッション状態から最新の印を取得（初期化）
                if race_id not in st.session_state.user_marks:
                    st.session_state.user_marks[race_id] = {h.get('number'): h.get('expert_mark', '無') for h in all_horses}
                
                for h in all_horses:
                    # 前走情報の取得
                    last_run = h.get('last_run', {})
                    last_rank = last_run.get('rank', '-')
                    num = h.get('number', 0)
                    
                    # ユーザー設定の印（なければAI印）
                    current_mark = st.session_state.user_marks[race_id].get(num, h.get('expert_mark', '無'))
                    
                    # カラーロジック用の計算
                    om_rank = h.get('omega_rank', 99)
                    pop_rank = h.get('popularity_rank', 99)
                    bug_deg = h.get('bug_degree', 0)
                    
                    df_data.append({
                        "My印": current_mark,
                        "馬番・馬名": f"{num}. {h.get('name', '')}",
                        "馬番": num, # 内部処理用
                        "AI印": h.get('expert_mark', '無'),
                        "Ω指数": h.get('omega', 0),
                        "Ω順": h.get('omega_rank', '-'),
                        "人気": h.get('popularity', '-'),
                        "人順": h.get('popularity_rank', '-'),
                        "バグ": bug_deg,
                        "評価": h.get('performance_judgment', ''),
                        "理由": h.get('performance_summary', ''),
                        "ジョッキー": h.get('jockey', ''),
                        "斤量": h.get('kinryo', 0),
                        "増減": h.get('kinryo_diff', ''),
                        "間隔": h.get('interval', ''),
                        "ブリンカ": h.get('blinker', ''),
                        "馬体重": h.get('weight_info', ''),
                        "コース実績": h.get('course_record', ''),
                        "距離実績": h.get('distance_record', ''),
                        "馬場実績": h.get('surface_record', ''),
                        "ローテ実績": h.get('rotation_record', ''),
                        "季節実績": h.get('season_record', ''),
                        "騎手相性": h.get('jockey_record', ''),
                        "枠実績": h.get('frame_record', ''),
                        "展開走": h.get('pace_record', ''),
                        "ベストT": h.get('best_time', ''),
                        "上がり3F": h.get('best_3f', ''),
                        "前走着": last_rank,
                        "点数": round(h.get('total_score', 0), 1)
                    })
                df = pd.DataFrame(df_data)
                
                # --- タイム順位の事前計算 ---
                def to_seconds(val):
                    if not val or val == '-': return float('inf')
                    s_val = str(val).strip()
                    parts = s_val.split(':')
                    try:
                        if len(parts) > 1: return float(parts[0])*60 + float(parts[1])
                        nums = re.findall(r"(\d+\.?\d*)", s_val)
                        return float(nums[0]) if nums else float('inf')
                    except: return float('inf')

                t_secs = df['ベストT'].apply(to_seconds)
                a_secs = df['上がり3F'].apply(to_seconds)
                t_valid = t_secs[t_secs != float('inf')]
                a_valid = a_secs[a_secs != float('inf')]
                t_ranks = t_secs.rank(method='min') if len(t_valid) > 0 else pd.Series([None]*len(df))
                a_ranks = a_secs.rank(method='min') if len(a_valid) > 0 else pd.Series([None]*len(df))
                max_t = t_ranks[t_secs != float('inf')].max() if len(t_valid) > 0 else None
                max_a = a_ranks[a_secs != float('inf')].max() if len(a_valid) > 0 else None

                # --- 配色関数 ---
                def get_record_color(val):
                    """成績文字列から背景色を返す"""
                    if not val or not isinstance(val, str) or val in ('-', 'ー'): return ''
                    m = re.match(r'(\d+)-(\d+)-(\d+)-(\d+)', val)
                    if not m: return ''
                    parts = [int(x) for x in m.groups()]
                    total = sum(parts)
                    if total == 0: return '#d3d3d3'
                    rate = (parts[0] + parts[1] + parts[2]) / total
                    if rate >= 0.75: return '#ffb3b3'
                    if rate >= 0.50: return '#ffffb3'
                    if rate > 0.25: return '#b3d9ff'
                    return '#d3d3d3'

                def get_time_color(rank, max_rank):
                    """タイム順位から背景色を返す"""
                    if rank is None or max_rank is None or pd.isna(rank): return ''
                    if rank == 1: return '#ffb3b3'
                    if rank == 2: return '#ffffb3'
                    if rank == max_rank: return '#d3d3d3'
                    if rank == max_rank - 1 and max_rank > 1: return '#b3d9ff'
                    return ''

                # --- HTMLテーブルを直接生成 (確実に着色を反映) ---
                record_cols = ['コース実績', '距離実績', '馬場実績', 'ローテ実績', '季節実績', '騎手相性', '枠実績', '展開走']
                # 表示するカラム（馬番は非表示）
                display_cols = [c for c in df.columns if c != '馬番']
                
                html = '<div style="overflow-x:auto;"><table style="border-collapse:collapse; width:100%; font-size:0.85rem;">'
                # ヘッダー
                html += '<tr>'
                for col in display_cols:
                    html += f'<th style="border:1px solid #555; padding:4px 6px; background:#1a1a2e; color:#eee; white-space:nowrap; position:sticky; top:0; z-index:1;">{col}</th>'
                html += '</tr>'
                # データ行
                for idx, row in df.iterrows():
                    html += '<tr>'
                    for col in display_cols:
                        val = row[col]
                        bg = ''
                        color = '#eee'
                        # 成績カラム
                        if col in record_cols:
                            bg = get_record_color(val)
                            if bg: color = '#111'
                        # タイムカラム
                        elif col == 'ベストT':
                            bg = get_time_color(t_ranks[idx], max_t)
                            if bg: color = '#111'
                        elif col == '上がり3F':
                            bg = get_time_color(a_ranks[idx], max_a)
                            if bg: color = '#111'
                        # AI印
                        elif col == 'AI印':
                            if val == '◎': bg = '#ffb3b3'; color = '#111'
                            elif val in ('○', '◯'): bg = '#ffffb3'; color = '#111'
                        # バグ
                        elif col == 'バグ':
                            try:
                                if int(val) >= 5: bg = '#ffb3b3'; color = '#111'
                            except: pass
                        
                        style = f'border:1px solid #555; padding:3px 5px; white-space:nowrap; color:{color};'
                        if bg:
                            style += f' background-color:{bg};'
                        # My印と馬番・馬名は固定風
                        if col in ('My印', '馬番・馬名'):
                            style += ' font-weight:bold;'
                        html += f'<td style="{style}">{val}</td>'
                    html += '</tr>'
                html += '</table></div>'
                
                st.markdown(html, unsafe_allow_html=True)
                  
                # My印の編集機能
                with st.expander("📝 My印を更新する"):
                    cols_mark = st.columns(4)
                    for i, h in enumerate(all_horses):
                        with cols_mark[i % 4]:
                            num = h['number']
                            current = st.session_state.user_marks[race_id].get(num, '無')
                            new_mark = st.selectbox(
                                f"{num}. {h['name']}",
                                ["◎", "○", "▲", "△", "☆", "×", "無"],
                                index=["◎", "○", "▲", "△", "☆", "×", "無"].index(current),
                                key=f"mark_select_{race_id}_{num}"
                            )
                            if new_mark != current:
                                st.session_state.user_marks[race_id][num] = new_mark
                                st.rerun()

            # --- その他の馬 (Other Horses) ---
            st.divider()
            st.markdown("### 🔍 その他の有力馬・伏兵")
            others = analysis.get('other_horses', [])
            if others:
                # スコア順に表示
                for h in others:
                    with st.expander(f"{h['number']}番 {h['name']} (期待値スコア: {h['score']})"):
                        st.write(f"**分析理由:** {h['reason']}")

            # 展開予想・買い目
            st.divider()
            with st.expander("📈 展開予想分析 & 具体的買い目戦略", expanded=True):
                st.markdown(f"#### 🏟️ 展開予想\n{analysis.get('pace_analysis', '展開データなし')}")
                st.markdown("#### 💰 推奨買い目戦略")
                st.info(analysis.get('strategy', '解析中です...'))

st.sidebar.divider()
st.sidebar.caption("© 2026 Keiba Lab Web Simplified Edition")
st.sidebar.caption("Data source: KeibaLab")
