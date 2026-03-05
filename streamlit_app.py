import streamlit as st
import pandas as pd
from scraper import KeibaLabScraper
import datetime
import json
import re

# ページ設定
st.set_page_config(
    page_title="Keiba Lab Web (Simplified)",
    page_icon="🏇",
    layout="wide",
    initial_sidebar_state="expanded"
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

# サイドバー
st.sidebar.title("🏇 Keiba Lab Web")
st.sidebar.markdown("Simplified Prediction Engine")

# 日付選択
selected_date = st.sidebar.date_input("開催日選択", datetime.date.today())
date_str = selected_date.strftime("%Y%m%d")

st.sidebar.divider()

# メインエリア
st.title(f"📍 {selected_date.strftime('%Y/%m/%d')} の開催レース")

# レース一覧取得
with st.spinner("レース一覧を取得中..."):
    try:
        res = st.session_state.scraper.get_races_by_date(date_str, skip_scoring=True)
        races = res.get('races', [])
    except Exception as e:
        st.error(f"エラーが発生しました: {e}")
        races = []

if not races:
    st.info("該当日のレースが見つかりません。")
else:
    # 会場ごとにグループ化
    venues = sorted(list(set(r.get('venue') for r in races if r.get('venue'))))
    
    selected_venue = st.sidebar.selectbox("会場絞り込み", ["すべて"] + venues)
    
    display_races = races
    if selected_venue != "すべて":
        display_races = [r for r in races if r.get('venue') == selected_venue]

    # レース選択
    race_options = {f"{r['venue']}{r['race_num']}R {r['name']}": r for r in display_races}
    selected_race_label = st.selectbox("分析するレースを選択してください", list(race_options.keys()))
    
    if selected_race_label:
        race = race_options[selected_race_label]
        race_id = race['id']
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("🏁 レース情報")
            st.write(f"**{race['name']}**")
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
                        <div style="background: rgba(255,255,255,0.05); padding: 15px; border-radius: 10px; border-left: 5px solid {color};">
                            <h3 style="margin:0; color:{color};">{mark} {horse['name']}</h3>
                            <p style="margin:5px 0; font-size:1.2rem; font-weight:bold;">{horse['number']}番 ({horse['judgment']})</p>
                            <p style="margin:0; font-size:0.9rem; color:#aaa;">Ω指数: <span style="color:#fff; font-weight:bold;">{horse['omega']}</span></p>
                        </div>
                        """, unsafe_allow_html=True)
                        st.caption(f"理由: {horse.get('reason', '')}")

            # --- ヒモ馬 (Himo Horses) ---
            st.markdown("### 🔗 ヒモ馬候補 (5頭)")
            himo = analysis.get('himo_horses', [])
            if himo:
                cols_himo = st.columns(5)
                for i, horse in enumerate(himo[:5]):
                    with cols_himo[i]:
                        st.markdown(f"""
                        <div style="background: rgba(255,255,255,0.03); padding: 10px; border-radius: 8px; border-bottom: 2px solid #27ae60; text-align: center;">
                            <div style="font-weight:bold; font-size:1.1rem;">{horse['number']}. {horse['name']}</div>
                            <div style="font-size:0.8rem; color:#aaa;">{horse['judgment']}</div>
                        </div>
                        """, unsafe_allow_html=True)
                        st.caption(horse.get('reason', ''))

            # --- 消し馬 (Eliminated Horses) ---
            st.markdown("### ❌ 消し馬 (除外推奨3頭)")
            discouraged = analysis.get('discouraged', [])
            if discouraged:
                cols_keshi = st.columns(3)
                for i, horse in enumerate(discouraged[:3]):
                    with cols_keshi[i]:
                        st.markdown(f"""
                        <div style="background: rgba(231,76,60,0.05); padding: 10px; border-radius: 8px; border-left: 3px solid #888;">
                            <strong>{horse['number']}番 {horse['name']}</strong>
                            <div style="font-size:0.85rem; color:#ccc; margin-top:4px;">理由: {horse.get('reason', '')}</div>
                        </div>
                        """, unsafe_allow_html=True)

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
                        "馬番": num,
                        "My印": current_mark,
                        "AI印": h.get('expert_mark', '無'),
                        "馬名": h.get('name', ''),
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
                
                # --- 配色ロジック (Pandas Styler) ---
                def apply_styles(row):
                    styles = [''] * len(row)
                    idx = row.name
                    
                    # 専門家評価 (AI印)
                    mark = row['AI印']
                    if mark == '◎': styles[df.columns.get_loc('AI印')] = 'background-color: rgba(244, 67, 54, 0.3)'
                    elif mark == '○': styles[df.columns.get_loc('AI印')] = 'background-color: rgba(255, 193, 7, 0.3)'
                    elif mark == '▲': styles[df.columns.get_loc('AI印')] = 'background-color: rgba(0, 188, 212, 0.3)'
                    
                    # 評価
                    judg = row['評価']
                    if judg == '絶対買い': styles[df.columns.get_loc('評価')] = 'background-color: rgba(244, 67, 54, 0.3)'
                    elif judg == '買い': styles[df.columns.get_loc('評価')] = 'background-color: rgba(255, 193, 7, 0.3)'
                    elif judg == '見送り': styles[df.columns.get_loc('評価')] = 'background-color: rgba(117, 117, 117, 0.3)'

                    # Ω順・人順
                    if isinstance(row['Ω順'], int) and row['Ω順'] <= 3:
                        styles[df.columns.get_loc('Ω順')] = 'background-color: rgba(255, 193, 7, 0.2)'
                    if isinstance(row['人順'], int) and row['人順'] <= 3:
                        styles[df.columns.get_loc('人順')] = 'background-color: rgba(255, 193, 7, 0.2)'
                        
                    # バグ (人気とΩの乖離) - ローカル版の bg-red, bg-yellow ロジック
                    # (簡易的に正の値が大きい場合に色付け)
                    if row['バグ'] >= 5: styles[df.columns.get_loc('バグ')] = 'background-color: rgba(244, 67, 54, 0.3)'
                    elif row['バグ'] >= 3: styles[df.columns.get_loc('バグ')] = 'background-color: rgba(255, 193, 7, 0.3)'
                    
                    # 斤量増減
                    k_diff = row['増減']
                    try:
                        k_val = float(k_diff.replace('+', '')) if k_diff and k_diff != '±0' else 0
                        if k_val <= -2.0: styles[df.columns.get_loc('増減')] = 'background-color: rgba(244, 67, 54, 0.3)' # 減量
                        elif k_val >= 2.0: styles[df.columns.get_loc('増減')] = 'background-color: rgba(117, 117, 117, 0.3)' # 増量
                    except: pass
                    
                    return styles

                styled_df = df.style.apply(apply_styles, axis=1)

                # 表示と編集
                edited_df = st.data_editor(
                    styled_df, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "馬番": st.column_config.NumberColumn(width="small", pinned=True, disabled=True),
                        "My印": st.column_config.SelectboxColumn(
                            width="small", 
                            pinned=True, 
                            options=["◎", "○", "▲", "△", "☆", "×", "無"]
                        ),
                        "馬名": st.column_config.TextColumn(width="medium", pinned=True, disabled=True),
                        "AI印": st.column_config.TextColumn(width="small", disabled=True),
                        "点数": st.column_config.NumberColumn(disabled=True),
                        "理由": st.column_config.TextColumn(width="large", disabled=True),
                    },
                    key=f"editor_{race_id}"
                )
                  
                # 編集内容をセッション状態に保存
                if edited_df is not None:
                    # edited_df が Styler の場合は .data で元のDFを取得
                    target_df = edited_df if isinstance(edited_df, pd.DataFrame) else edited_df
                    new_marks = {row["馬番"]: row["My印"] for _, row in target_df.iterrows()}
                    st.session_state.user_marks[race_id] = new_marks

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
