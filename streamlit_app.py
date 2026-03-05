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
                for h in all_horses:
                    # 前走情報の取得
                    last_run = h.get('last_run', {})
                    last_rank = last_run.get('rank', '-')
                    
                    df_data.append({
                        "馬番": h.get('number', 0),
                        "印": h.get('expert_mark', '無'),
                        "馬名": h.get('name', ''),
                        "Ω指数": h.get('omega', 0),
                        "Ω順": h.get('omega_rank', '-'),
                        "人気": h.get('popularity', '-'),
                        "人順": h.get('popularity_rank', '-'),
                        "バグ": h.get('bug_degree', 0),
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
                
                # スタイル適用（条件付き書式などは st.dataframe で制限があるが、背景色などは pandas styler で可能）
                def highlight_omega(val):
                    color = 'background-color: rgba(241, 196, 15, 0.2)' if val >= 80 else ''
                    return color

                # 表示
                st.dataframe(
                    df, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "馬番": st.column_config.NumberColumn(width="small"),
                        "印": st.column_config.TextColumn(width="small"),
                        "馬名": st.column_config.TextColumn(width="medium"),
                        "理由": st.column_config.TextColumn(width="large"),
                    }
                )

            # 展開予想・買い目
            st.divider()
            with st.expander("📈 展開予想分析 & 具体的買い目戦略", expanded=True):
                st.markdown(f"#### 🏟️ 展開予想\n{analysis.get('pace_analysis', '展開データなし')}")
                st.markdown("#### 💰 推奨買い目戦略")
                st.info(analysis.get('strategy', '解析中です...'))

st.sidebar.divider()
st.sidebar.caption("© 2026 Keiba Lab Web Simplified Edition")
st.sidebar.caption("Data source: KeibaLab")
