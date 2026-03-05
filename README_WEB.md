# Keiba Lab Web 無料デプロイ手順

この手順に従うことで、Streamlit Cloud を利用して、現在の競馬予想エンジンを Web で無料で公開・利用できます。

## 必要なもの
1. [GitHub](https://github.com/) アカウント
2. [Streamlit Cloud](https://streamlit.io/cloud) アカウント（GitHub アカウントでサインアップ可能）

## デプロイ手順

### 1. GitHub リポジトリの準備
1. GitHub で新しいリポジトリ（例: `keiba-web`）を作成します。
2. 作成したリポジトリに以下のファイルをアップロードします：
   - `streamlit_app.py`
   - `scraper.py`
   - `requirements.txt`

### 2. Streamlit Cloud でのデプロイ
1. [Streamlit Cloud](https://share.streamlit.io/) にログインします。
2. 「Create app」または「New app」をクリックします。
3. リポジトリの選択画面で、先ほど作成した `keiba-web` を選択します。
4. 設定を以下のように入力します：
   - **Main file path**: `streamlit_app.py`
5. 「Deploy!」をクリックします。

### 3. 公開完了
- 数分でビルドが完了し、`https://xxx.streamlit.app` のような URL で Web アプリが公開されます。
- 以降、スマホや PC のブラウザからいつでも当日の予想を確認できます。

## 簡略版の仕様
- **当日の予想に特化**: DB を持たないため、過去の的中検証や履歴保存はできません。
- **データ取得**: 実行時に KeibaLab から最新データを取得して解析します。

> [!NOTE]
> 公開範囲を制限したい場合は、GitHub リポジトリを Private に設定し、Streamlit Cloud 側でも権限を制限することで自分専用にできます。
