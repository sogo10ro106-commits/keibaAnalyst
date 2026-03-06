import sqlite3
import json
import os
from datetime import datetime, timedelta

class DatabaseManager:
    def __init__(self, db_path='keibalab_cache.db'):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._get_connection() as conn:
            # races: レース一覧表用
            conn.execute('''
                CREATE TABLE IF NOT EXISTS races (
                    date_str TEXT PRIMARY KEY,
                    data_json TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # analysis: 個別レース分析結果用
            conn.execute('''
                CREATE TABLE IF NOT EXISTS analysis (
                    race_id TEXT PRIMARY KEY,
                    race_date TEXT,
                    data_json TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # results: レース結果判定用
            conn.execute('''
                CREATE TABLE IF NOT EXISTS results (
                    race_id TEXT PRIMARY KEY,
                    race_date TEXT,
                    data_json TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()

    def _is_stale(self, updated_at_str, race_date_str):
        """データの鮮度判定。
        レース日が昨日以前なら永久保存（stale=False）。
        レース日が今日以降なら、取得から2時間経過で stale=True と判定。
        """
        try:
            updated_at = datetime.strptime(updated_at_str, '%Y-%m-%d %H:%M:%S')
            race_date = datetime.strptime(race_date_str.replace('-', ''), '%Y%m%d').date()
            today = datetime.now().date()

            if race_date < today:
                return False  # 過去データは永久保存
            
            # 当日以降は2時間で更新
            return datetime.now() - updated_at > timedelta(hours=2)
        except Exception as e:
            print(f"Error checking staleness: {e}")
            return True

    def get_cached_races(self, date_str, force_refresh=False):
        if force_refresh: return None
        try:
            with self._get_connection() as conn:
                cursor = conn.execute('SELECT data_json, updated_at FROM races WHERE date_str = ?', (date_str,))
                row = cursor.fetchone()
                if row:
                    data_json, updated_at_str = row
                    if not self._is_stale(updated_at_str, date_str):
                        return json.loads(data_json)
        except Exception as e:
            print(f"DB Read Error (races): {e}")
        return None

    def save_races(self, date_str, data):
        try:
            with self._get_connection() as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO races (date_str, data_json, updated_at) VALUES (?, ?, ?)',
                    (date_str, json.dumps(data), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                )
                conn.commit()
        except Exception as e:
            print(f"DB Write Error (races): {e}")

    def get_cached_analysis(self, race_id, force_refresh=False):
        if force_refresh: return None
        try:
            with self._get_connection() as conn:
                cursor = conn.execute('SELECT data_json, updated_at, race_date FROM analysis WHERE race_id = ?', (race_id,))
                row = cursor.fetchone()
                if row:
                    data_json, updated_at_str, race_date_str = row
                    if not self._is_stale(updated_at_str, race_date_str):
                        return json.loads(data_json)
        except Exception as e:
            print(f"DB Read Error (analysis): {e}")
        return None

    def save_analysis(self, race_id, race_date, data):
        try:
            with self._get_connection() as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO analysis (race_id, race_date, data_json, updated_at) VALUES (?, ?, ?, ?)',
                    (race_id, race_date, json.dumps(data), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                )
                conn.commit()
        except Exception as e:
            print(f"DB Write Error (analysis): {e}")

    def get_cached_result(self, race_id):
        # 結果判定は過去日のものが確定すれば変わることはないので原則永久
        try:
            with self._get_connection() as conn:
                cursor = conn.execute('SELECT data_json FROM results WHERE race_id = ?', (race_id,))
                row = cursor.fetchone()
                if row:
                    return json.loads(row[0])
        except Exception as e:
            print(f"DB Read Error (results): {e}")
        return None

    def save_result(self, race_id, race_date, data):
        try:
            with self._get_connection() as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO results (race_id, race_date, data_json, updated_at) VALUES (?, ?, ?, ?)',
                    (race_id, race_date, json.dumps(data), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                )
                conn.commit()
        except Exception as e:
            print(f"DB Write Error (results): {e}")

    def clear_cache_by_date(self, date_str):
        """指定日付のキャッシュを削除する
        date_str: YYYYMMDD形式の日付文字列
        """
        deleted = {'races': 0, 'analysis': 0, 'results': 0}
        try:
            # analysis/resultsテーブル用に YYYY-MM-DD 形式を作成
            if len(date_str) == 8 and date_str.isdigit():
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            else:
                formatted_date = date_str

            with self._get_connection() as conn:
                # racesテーブル: date_strがキー (YYYYMMDD)
                cursor = conn.execute('DELETE FROM races WHERE date_str = ?', (date_str,))
                deleted['races'] = cursor.rowcount

                # analysisテーブル: race_dateカラムで絞込み (YYYY-MM-DD)
                cursor = conn.execute('DELETE FROM analysis WHERE race_date = ?', (formatted_date,))
                deleted['analysis'] = cursor.rowcount

                # resultsテーブル: race_dateカラムで絞込み (YYYY-MM-DD)
                cursor = conn.execute('DELETE FROM results WHERE race_date = ?', (formatted_date,))
                deleted['results'] = cursor.rowcount

                conn.commit()
            return deleted
        except Exception as e:
            print(f"DB Delete Error (by date): {e}")
            return None

    def get_cache_stats(self):
        """キャッシュの日付別統計を返す"""
        stats = []
        try:
            with self._get_connection() as conn:
                # racesテーブルから日付一覧
                cursor = conn.execute('''
                    SELECT r.date_str, r.updated_at,
                           (SELECT COUNT(*) FROM analysis a WHERE a.race_date = r.date_str) as analysis_count,
                           (SELECT COUNT(*) FROM results res WHERE res.race_date = r.date_str) as result_count
                    FROM races r
                    ORDER BY r.date_str DESC
                ''')
                for row in cursor.fetchall():
                    date_str, updated_at, analysis_count, result_count = row
                    stats.append({
                        'date': date_str,
                        'updated_at': updated_at,
                        'analysis_count': analysis_count,
                        'result_count': result_count
                    })
        except Exception as e:
            print(f"DB Stats Error: {e}")
        return stats

    def clear_cache(self):
        """全キャッシュを削除（手動メンテナンス用）"""
        try:
            with self._get_connection() as conn:
                conn.execute('DELETE FROM races')
                conn.execute('DELETE FROM analysis')
                conn.execute('DELETE FROM results')
                conn.commit()
            return True
        except:
            return False
