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
            st.subheader("📊 分析結果サマリー")
            
            # 推奨馬 (◎, ○, ▲)
            recs = analysis.get('recommendations', [])
            if recs:
                cols = st.columns(len(recs[:3]))
                for i, horse in enumerate(recs[:3]):
                    with cols[i]:
                        mark = get_mark(i+1)
                        st.metric(f"{mark} {horse['name']}", f" {horse['number']}番", delta=f"Ω {horse['omega']}")
                        st.caption(f"理由: {horse.get('reason', '')}")

            # 詳細テーブル
            st.markdown("### 🏇 各馬の分析一覧")
            all_horses = analysis.get('full_results', [])
            if all_horses:
                df_data = []
                for idx, h in enumerate(all_horses):
                    df_data.append({
                        "印": h.get('mark_text', ''),
                        "馬番": h.get('number', 0),
                        "馬名": h.get('name', ''),
                        "Ω指数": h.get('omega', 0),
                        "人気": h.get('popularity', '-'),
                        "ジョッキー": h.get('jockey', ''),
                        "推奨理由": h.get('reason', '')
                    })
                df = pd.DataFrame(df_data)
                st.dataframe(df, use_container_width=True, hide_index=True)

            # 展開予想・買い目
            with st.expander("📈 展開予想 & 買い目戦略"):
                st.write(analysis.get('pace_analysis', '展開データなし'))
                st.write("**推奨買い目:**")
                st.info(analysis.get('strategy', '解析中です...'))

st.sidebar.divider()
st.sidebar.caption("© 2026 Keiba Lab Web Simplified Edition")
st.sidebar.caption("Data source: KeibaLab")
