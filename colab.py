# 導入必要的函式庫
from google.colab import auth
from google.colab import drive
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.api_core import retry
import pandas as pd
import numpy as np
import datetime
import re
import time
import logging
from typing import List, Dict, Union, Optional

# 版本資訊
__version__ = '1.0.0'

# 設定基本日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 建立logger實例
logger = logging.getLogger(__name__)

# 認證 Google 帳號並掛載 Google Drive
try:
    auth.authenticate_user()
    drive.mount('/content/drive')
    print("Google Drive 掛載成功")
except Exception as e:
    print(f"掛載失敗: {str(e)}")
    raise Exception("Google Drive 掛載失敗，請檢查權限設定")

# 使用者輸入必要的ID
def validate_sheet_id(sheet_id: str, sheet_name: str) -> str:
    """驗證輸入的ID不為空"""
    if not sheet_id or not isinstance(sheet_id, str):
        raise ValueError(f"{sheet_name} ID不能為空")
    return sheet_id.strip()


try:
    folder_id = validate_sheet_id(
        input("請輸入要掃描試算表的資料夾ID："),
        "資料夾"
    )

    statistics_sheet_id = validate_sheet_id(
        input("請輸入統計資料表ID："),
        "統計資料表"
    )

    account_query_sheet_id = validate_sheet_id(
        input("請輸入查詢挂帳試算表ID："),
        "查詢挂帳試算表"
    )

    customer_code_sheet_id = validate_sheet_id(
        input("請輸入查詢客戶供應商代號和傳票類別試算表ID："),
        "客戶供應商代號表"
    )

    product_code_sheet_id = validate_sheet_id(
        input("請輸入產品代號試算表ID："),
        "產品代號試算表"
    )

    print("所有ID輸入完成")

except ValueError as e:
    print(f"輸入錯誤: {str(e)}")
    raise
except Exception as e:
    print(f"發生未預期的錯誤: {str(e)}")
    raise

# 建立 Google Drive 和 Sheets 服務
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError

try:
    # 取得認證
    credentials = GoogleCredentials.get_application_default()

    # 建立服務
    drive_service = build('drive', 'v3', credentials=credentials)
    sheets_service = build('sheets', 'v4', credentials=credentials)

    # 測試連線是否成功
    try:
        # 測試讀取資料夾內容
        folder_results = drive_service.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'",
            fields="files(id, name)",
            pageSize=1000  # 確保能獲取足夠的檔案
        ).execute()

        files = folder_results.get('files', [])

        if not files:
            raise ValueError("指定的資料夾內沒有試算表")

        print("成功連接到指定資料夾")
        print(f"資料夾內試算表數量: {len(files)}")
        print("試算表列表：")
        for file in files:
            print(f"- {file['name']}")

    except HttpError as e:
        if e.resp.status == 404:
            print(f"找不到指定的資料夾，請確認資料夾ID是否正確")
        elif e.resp.status == 403:
            print(f"沒有存取權限，請確認權限設定")
        else:
            print(f"API錯誤: {str(e)}")
        raise
    except Exception as e:
        print(f"存取資料夾時發生錯誤: {str(e)}")
        raise

except Exception as e:
    print(f"服務建立失敗: {str(e)}")
    raise

def extract_filtered_column_from_sheets(files, sheets_service,
                                        target_column_name="品　種",
                                        time_column_name="時間"):
    skip_keywords = {"現金", "MASTER", "VISA", "挂帳", "Visa", "AE", "Master", "JCB", "匯款", "卦帳"}

    for file in files:
        spreadsheet_id = file['id']
        spreadsheet_name = file['name']

        try:
            # 取得第一個工作表名稱
            sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheet_title = sheet_metadata['sheets'][0]['properties']['title']

            # 讀取該工作表內容
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_title
            ).execute()

            values = result.get('values', [])

            if not values:
                print(f"[{spreadsheet_name}] 沒有資料")
                continue

            headers = values[0]
            if target_column_name not in headers or time_column_name not in headers:
                print(f"[{spreadsheet_name}] 缺少 '{target_column_name}' 或 '{time_column_name}' 欄位")
                continue

            col_index = headers.index(target_column_name)
            time_index = headers.index(time_column_name)

            print(f"\n[{spreadsheet_name}] 的篩選後「{target_column_name}」欄位資料如下：")

            for row in values[1:]:  # 跳過標題列
                # 檢查欄位是否存在與非空
                if len(row) > col_index and len(row) > time_index:
                    variety = row[col_index].strip()
                    time_val = row[time_index].strip()

                    if variety in skip_keywords:
                        continue
                    if time_val == "":
                        continue

                    print(f"- {variety}")

        except Exception as e:
            print(f"讀取 [{spreadsheet_name}] 時發生錯誤: {str(e)}")


def extract_filtered_column_from_sheets(files, sheets_service, target_column_name="品　種"):
    skip_keywords = ["visa", "master", "ae"]
    results = []

    for file in files:
        spreadsheet_id = file['id']
        spreadsheet_name = file['name']

        try:
            metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheet_title = metadata['sheets'][0]['properties']['title']

            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_title
            ).execute()
            values = result.get("values", [])

            if not values:
                continue

            headers = values[0]
            if target_column_name not in headers:
                continue

            col_index = headers.index(target_column_name)

            for row in values[1:]:
                if len(row) <= col_index:
                    continue
                raw_value = row[col_index].strip()
                norm_value = normalize_product_name(raw_value)

                # 檢查是否包含任何關鍵字
                if any(keyword in norm_value for keyword in skip_keywords):
                    continue

                results.append(raw_value)  # 保留原始值

        except Exception as e:
            print(f"[{spreadsheet_name}] 錯誤: {str(e)}")

    return results

import re
import time
from googleapiclient.errors import HttpError

def normalize_product_name(name):
  """
  標準化商品名稱：
  - 移除所有空白（半形、全形）
  - 將中文括號替換為英文括號
  - 去除不可見字元
  - 轉為小寫（便於比對）
  """
  name = name.strip()
  name = name.replace(" ", "").replace("\u3000", "")  # 半形空格、全形空格
  name = name.replace("（", "(").replace("）", ")")    # 中文括號轉英文
  name = re.sub(r"[\u200b\u200e\u202c]", "", name)    # 隱藏字元
  return name.lower()


def load_product_code_mapping(sheets_service, product_code_sheet_id):
  try:
      result = sheets_service.spreadsheets().values().get(
          spreadsheetId=product_code_sheet_id,
          range="Sheet2!B:C"  # 抓 B 和 C 欄
      ).execute()
      values = result.get("values", [])
      mapping = {}

      for row in values[1:]:  # 跳過標題列
          if len(row) >= 2:
              code = row[0].strip()                       # B欄：代號
              name = row[1].strip()                       # C欄：名稱
              normalized_name = normalize_product_name(name)  # 進行標準化處理
              mapping[normalized_name] = code             # 建立映射：標準化名稱 → 代號

      return mapping
  except Exception as e:
      print(f"載入產品代號表失敗: {str(e)}")
      return {}


def collect_statistics_data(files, sheets_service, product_mapping, account_mapping,
                          target_column_name="品　種", time_column_name="時間"):
    skip_keywords = {
        normalize_product_name(k) for k in [
            "現金", "MASTER", "VISA", "挂帳", "Visa", "AE", "Master", "挂帳",
            "jcb", "匯款", "訂金", "銀聯"
        ]
    }

    output_rows = []
    special_vendor_dates = []
    special_vendor_codes = {'52', '53', '54', '55'}

    def process_vendor_code(code):
        """處理客供商代號：除了特殊代號外，都補滿六位數"""
        code = code.strip()
        if code in special_vendor_codes:
            return code
        return code.zfill(6)

    def get_tax_code(voucher_type):
        """根據傳票類別決定稅別"""
        if voucher_type in ['S994', 'S997']:
            return "2"
        return "6"

    for file in files:
        spreadsheet_id = file['id']
        spreadsheet_name = file['name']
        first_entry_written = False

        try:
            # 例子：23210225002 -> 從第5碼開始取 0225
            m = re.search(r'^.{4}(\d{4})', spreadsheet_name)
            if m:
                month_day = m.group(1)  # 取得 "0225"
                month = month_day[:2]
                day = month_day[2:]
                spreadsheet_date = f"114/{month}/{day}"
            else:
                spreadsheet_date = ""
        except:
            spreadsheet_date = ""

        try:
            metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheet_title = metadata['sheets'][0]['properties']['title']

            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_title
            ).execute()
            values = result.get("values", [])

            if not values:
                continue
            headers = values[0]
            if target_column_name not in headers or time_column_name not in headers:
                continue

            col_index = headers.index(target_column_name)
            time_index = headers.index(time_column_name)
            unit_price_index = headers.index("單價") if "單價" in headers else None
            category_index = headers.index("類別") if "類別" in headers else None
            quantity_index = headers.index("數量") if "數量" in headers else None
            amount_index = headers.index("金額") if "金額" in headers else None
            invoice_index = headers.index("發票") if "發票" in headers else None
            reason_index = headers.index("贈送原因") if "贈送原因" in headers else None

            all_product_raws = []
            default_vendor_code = "000999"  # 預設客供商代號
            vendor_code_lookup = None

            for row in values[1:]:
                if len(row) > col_index:
                    all_product_raws.append(row[col_index].strip())

            all_product_raws = [row[col_index].strip() for row in values[1:] if len(row) > col_index]
            payment_method = "未分類"

            # 統計付款方式種類（不分大小寫）
            all_methods_detected = set()
            for p in all_product_raws:
                pl = p.lower()
                if "現金" in pl:
                    all_methods_detected.add("現金")
                elif "挂帳" in pl or "挂帳" in pl:
                    all_methods_detected.add("挂帳")
                elif any(k in pl for k in ["visa", "master", "ae", "jcb", "銀聯"]):
                    all_methods_detected.add("信用卡")
                elif "匯款" in pl or "訂金" in pl:
                    all_methods_detected.add("匯款")

            # 設定付款方式邏輯
            if len(all_methods_detected) == 1:
                payment_method = list(all_methods_detected)[0]
            else:
                payment_method = "多種"

            # 掛帳處理（抓取客戶代號）
            if "挂帳" in all_methods_detected and category_index is not None:
                for row in values[1:]:
                    if len(row) > max(col_index, category_index):
                        if row[col_index].strip() in ["挂帳", "挂帳"]:
                            vendor_code_raw = row[category_index].strip()
                            default_vendor_code = process_vendor_code(vendor_code_raw)
                            vendor_code_lookup = default_vendor_code
                            if vendor_code_raw in special_vendor_codes:
                                special_vendor_dates.append({
                                    'date': spreadsheet_date,
                                    'vendor_code': vendor_code_raw,
                                    'spreadsheet_name': spreadsheet_name
                                })
                            break

            total_amount_value = ""
            if amount_index is not None:
                for row in values[1:]:
                    if len(row) > col_index and row[col_index].strip() == "結帳 小計":
                        if len(row) > amount_index:
                            total_amount_value = row[amount_index].strip()
                        break

            first_entry = True
            for row in values[1:]:
                if len(row) <= max(col_index, time_index):
                    continue

                product_raw = row[col_index].strip()
                time_raw = row[time_index].strip()

                if normalize_product_name(product_raw) in skip_keywords or time_raw == "":
                    continue

                # 檢查單價和贈送原因
                current_vendor_code = default_vendor_code  # 每筆商品都從預設值開始
                is_pr_item = False

                # 檢查是否為公關品
                if reason_index is not None and len(row) > reason_index:
                    is_pr_item = row[reason_index].strip() == "公關品"

                is_service_fee = normalize_product_name(product_raw) == normalize_product_name("[服務費]")

                # 檢查單價
                if unit_price_index is not None and len(row) > unit_price_index:
                    try:
                        unit_price_str = row[unit_price_index].replace(",", "").strip()  # ✅ 先取文字
                        if unit_price_str == "":
                            if is_service_fee:
                                unit_price = 0.0  # 放行服務費即使沒填單價
                            else:
                                continue  # 空白單價跳過
                        else:
                            unit_price = float(unit_price_str)

                            if unit_price == 0:
                                if not is_pr_item:
                                    if is_service_fee:
                                        unit_price = 0.0  # 放行服務費
                                    else:
                                        continue  # 非公關品 & 不是服務費 → 跳過
                                else:
                                    current_vendor_code = "000995"  # 公關品（零單價）
                            else:
                                # 單價不為零的商品
                                if is_pr_item:
                                    current_vendor_code = "000995"
                                elif payment_method == "挂帳":
                                    current_vendor_code = default_vendor_code
                                else:
                                    current_vendor_code = "000999"
                    except ValueError:
                      if is_service_fee:
                        unit_price = 0.0  # ✅ 服務費就算錯也放行
                      else:
                        continue  # 其他錯誤跳過

                month_day = spreadsheet_date

                product_key = normalize_product_name(product_raw)
                product_code = product_mapping.get(product_key, f"未查到此商品({product_raw})")

                if product_raw.strip() == "[服務費]":
                    quantity = "1"
                elif quantity_index is not None and len(row) > quantity_index:
                    quantity = row[quantity_index].strip()
                else:
                    quantity = ""

                amount = row[amount_index].strip() if amount_index is not None and len(row) > amount_index else ""

                if amount:
                    amount = amount.replace(",", "")

                # 設定傳票類別
                if payment_method == "多種":
                    voucher_type = "S994"
                elif payment_method == "挂帳" and vendor_code_lookup:
                    voucher_type = account_mapping.get(vendor_code_lookup, "未查到")
                else:
                    voucher_type = {"現金": "S998", "信用卡": "S997", "匯款": "S996"}.get(payment_method, "S996")

                # 檢查是否為公關品並設定相應的傳票類別
                if is_pr_item:
                    voucher_type = ""

                # 根據傳票類別設定稅別
                tax_code = get_tax_code(voucher_type)

                if payment_method == "現金":
                    cash_amount = amount
                    card_amount = ""
                elif payment_method == "信用卡":
                    cash_amount = ""
                    card_amount = amount
                else:
                    cash_amount = ""
                    card_amount = ""

                total_amount = ""
                invoice_number_for_output = ""
                has_invoice_for_output = False

                if first_entry:
                    if total_amount:
                        try:
                            amt_int = int(total_amount.replace(",", ""))
                            tax_amount_to_write = str(round(amt_int - amt_int / 1.05))
                        except:
                            tax_amount_to_write = ""
                    tax_amount_to_write = ""
                    remarks_m250 = ""
                    invoice_number_for_output = ""
                    all_invoice_numbers = []

                    if invoice_index is not None:
                        for row_check in values[1:]:
                            if len(row_check) > invoice_index:
                                invoice_str = row_check[invoice_index].strip()
                                m = re.search(r"發票號:(\w+)", invoice_str)
                                if "發票金額:0" in invoice_str:
                                    continue
                                elif m:
                                    all_invoice_numbers.append(m.group(1))

                    # ✅ 正確的 has_invoice 判斷邏輯要放在發票收集完之後
                    has_invoice_for_output = bool(all_invoice_numbers)

                    # 格式化總金額
                    try:
                        amount_numeric = round(float(total_amount_value.replace(",", "")))
                        total_amount = str(amount_numeric).zfill(8)
                    except:
                        total_amount = ""

                    # 計算稅額（W = V - V/1.05）
                    tax_amount_to_write = ""

                    if total_amount:
                        try:
                            amt_int = int(total_amount)
                            untaxed = round(amt_int / 1.05)
                            tax_amount_to_write = str(amt_int - untaxed)
                        except:
                            tax_amount_to_write = ""
                    remarks_m250 = ""
                    invoice_number_for_output = ""
                    all_invoice_numbers = []
                    has_invoice_for_output = False

                    if invoice_index is not None:
                        for row_check in values[1:]:
                            if len(row_check) > invoice_index:
                                invoice_str = row_check[invoice_index].strip()
                                m = re.search(r"發票號:(\w+)", invoice_str)
                                if "發票金額:0" in invoice_str:
                                    continue
                                elif m:
                                    all_invoice_numbers.append(m.group(1))

                    if payment_method == "多種":
                        # 多付款方式：發票欄留空，備註寫全部發票號碼串接
                        invoice_number_for_output = ""  # ✅ 空白，不寫 "無發票"
                        remarks_m250 = "_".join(all_invoice_numbers) if all_invoice_numbers else ""
                        has_invoice_for_output = False
                    else:
                        # 單一付款方式：發票欄寫第一張發票號碼
                        invoice_number_for_output = all_invoice_numbers[0] if all_invoice_numbers else ""
                        remarks_m250 = ""  # ✅ 備註空白
                        has_invoice_for_output = invoice_number_for_output != ""

                    # 格式化總金額
                    try:
                        amount_numeric = round(float(total_amount_value.replace(",", "")))
                        total_amount = str(amount_numeric).zfill(8)
                    except:
                        total_amount = ""

                first_entry = False

                # 開發票判斷
                if invoice_index is not None and len(row) > invoice_index:
                    invoice_str = row[invoice_index].strip()
                    has_invoice = not invoice_str.startswith("發票金額:0")
                else:
                    has_invoice = False

                # 未稅邏輯計算（排除挂帳）
                untaxed_price = ""
                untaxed_amount = ""
                tax_amount = ""
                # 處理未稅單價（來自單價欄）
                # ✅ 改為無論是否有發票，都直接取原單價作為未稅單價
                if unit_price_index is not None and len(row) > unit_price_index:
                    try:
                        unit_price_val = float(row[unit_price_index].replace(",", ""))
                        untaxed_price = str(round(unit_price_val))
                    except:
                        untaxed_price = ""

                # 處理未稅單價（來自單價欄）
                if unit_price_index is not None and len(row) > unit_price_index:
                    if amount:
                        try:
                            amt = float(amount.replace(",", ""))
                            untaxed_amount = str(round(amt))
                            tax_amount = str(round(amt - amt / 1.05))
                        except:
                            untaxed_amount = ""
                            tax_amount = ""

                remarks_to_write = remarks_m250 if first_entry_written is False else ""
                invoice_number_to_write = invoice_number_for_output if not first_entry_written else ""
                tax_amount_to_write = ""
                invoice_amounts = []
                if invoice_index is not None:
                    for row_check in values[1:]:
                        if len(row_check) > invoice_index:
                            invoice_str = row_check[invoice_index].strip()
                            m_amt = re.search(r"發票金額:(\d+)", invoice_str)
                            if m_amt:
                                invoice_amounts.append(int(m_amt.group(1)))

                # W 欄：統計總稅額
                try:
                    total_invoice_amt = sum(invoice_amounts)
                    total_tax = str(round(total_invoice_amt - total_invoice_amt / 1.05))
                except:
                    total_tax = ""

                if not first_entry_written and total_amount:
                    try:
                        amt_int = int(total_amount)
                        untaxed = round(amt_int / 1.05)
                        tax_amount_to_write = str(amt_int - untaxed)
                    except:
                        tax_amount_to_write = ""

                # 新增總稅額（W欄）：總含稅金額 - (總含稅金額 / 1.05)
                total_tax = ""

                # W 欄：統計總稅額 ✅ 修正為從發票金額加總後計算
                invoice_amounts = []
                if invoice_index is not None:
                    for row_check in values[1:]:
                        if len(row_check) > invoice_index:
                            invoice_str = row_check[invoice_index].strip()
                            m_amt = re.search(r"發票金額:(\d+)", invoice_str)
                            if m_amt:
                                invoice_amounts.append(int(m_amt.group(1)))

                try:
                    total_invoice_amt = sum(invoice_amounts)
                    total_tax = str(round(total_invoice_amt - total_invoice_amt / 1.05))
                except:
                    total_tax = ""

                output_rows.append([
                    spreadsheet_name,         # B 銷貨單號（檔名）
                    product_code,             # E 產品代號
                    spreadsheet_date,         # G 銷貨日期（由檔名取出）
                    payment_method,           # R 付款方式
                    current_vendor_code,      # C 客供商代號
                    voucher_type,             # Z 傳票類別
                    quantity,                 # F 數量
                    tax_code,                 # S 稅別
                    cash_amount,              # T 付現金額
                    card_amount,              # U 刷卡金額
                    total_amount,             # V 含稅總金額
                    untaxed_price,            # AA 未稅單價
                    untaxed_amount,           # AB 未稅金額
                    tax_amount,               # AC：稅額，每一筆都寫
                    invoice_number_to_write,  # AK：發票號碼（只有首筆寫）
                    remarks_to_write,         # AW：備註（只有首筆寫）
                    total_tax if not first_entry_written else "",  # W 欄（總稅額）只在第一筆寫
                ])

                first_entry_written = True

        except Exception as e:
            print(f"[{spreadsheet_name}] 錯誤: {str(e)}")

    return output_rows, special_vendor_dates


def retry_on_error(func, max_retries=3, delay=5):
    """重試機制：處理暫時性的 API 錯誤"""
    for attempt in range(max_retries):
        try:
            return func()
        except HttpError as e:
            if e.resp.status in [403, 429, 503]:  # 配額限制或服務暫時不可用
                if attempt < max_retries - 1:  # 如果還有重試次數
                    print(f"遇到暫時性錯誤，{delay}秒後重試... (第{attempt + 1}次)")
                    time.sleep(delay)
                    delay *= 2  # 每次重試延遲時間加倍
                    continue
            raise  # 如果是其他錯誤或已達最大重試次數，則拋出異常

def write_to_statistics_sheet(sheets_service, sheet_id, data, start_row=2):
    try:
        aligned_data = []
        for row in data:
            row_out = [""] * 49  # 至少到 Z欄
            row_out[1] = row[0]   # B欄：銷貨單號
            row_out[2] = row[4]   # C欄：客供商代號
            row_out[4] = row[1]   # E欄：產品代號
            row_out[5] = row[6]   # F欄：數量
            row_out[6] = row[2]   # G欄：銷貨日期
            row_out[18] = row[7]  # X欄：稅別
            row_out[21] = row[10]  # V欄：總含稅金額
            row_out[28] = row[13]  # AC欄：稅額
            row_out[36] = row[14]  # AK欄：發票號碼
            row_out[48] = row[15]  # AW欄，備註M250
            row_out[25] = row[5]   # Z欄：傳票類別
            row_out[26] = row[11]  # AA欄：未稅單價
            row_out[27] = row[12]  # AB欄：未稅金額
            row_out[22] = row[16]  # W欄：總稅額
            row_out[28] = row[13]  # AC欄：稅額，每筆都填

            aligned_data.append(row_out)

        range_str = f"A{start_row}:AW"
        body = {"values": aligned_data}

        def update_sheet():
            return sheets_service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range_str,
                valueInputOption="RAW",
                body=body
            ).execute()

        response = retry_on_error(update_sheet)
        print(f"✅ 成功寫入統計資料表，共 {response.get('updatedRows', 0)} 筆")
    except Exception as e:
        print(f"寫入統計資料表時出錯: {str(e)}")



def load_account_mapping(sheets_service, account_query_sheet_id):
  try:
      result = sheets_service.spreadsheets().values().get(
          spreadsheetId=account_query_sheet_id,
          range="A:J"  # 假設表頭在第1列，帳號在B，傳票類別在J
      ).execute()
      values = result.get("values", [])
      mapping = {}

      for row in values[1:]:  # 跳過表頭
          if len(row) >= 10:  # 至少要有到J欄
              account_id = row[1].strip().zfill(6)  # B欄 → 補滿六位
              voucher_type = row[9].strip()         # J欄
              mapping[account_id] = voucher_type

      return mapping
  except Exception as e:
      print(f"載入挂帳傳票對照表失敗: {str(e)}")
      return {}


def write_special_vendor_dates(sheets_service, sheet_id, special_vendor_dates):
    """將特殊客供商代號（52、53、54、55）的使用日期寫入新工作表"""
    try:
        headers = ["日期", "客供商代號", "銷貨單號"]
        rows = [headers]

        for entry in special_vendor_dates:
            rows.append([
                entry['date'],
                entry['vendor_code'],
                entry['spreadsheet_name']
            ])

        def create_sheet():
            requests = [{
                'addSheet': {
                    'properties': {
                        'title': '特殊客供商記錄',
                        'gridProperties': {
                            'rowCount': max(len(rows), 100),
                            'columnCount': 3
                        }
                    }
                }
            }]

            try:
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={'requests': requests}
                ).execute()
            except Exception:
                pass  # 工作表可能已存在，忽略錯誤

        def update_data():
            range_str = "特殊客供商記錄!A1:C"
            body = {"values": rows}
            return sheets_service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range_str,
                valueInputOption="RAW",
                body=body
            ).execute()

        # 建立工作表（如果不存在）
        retry_on_error(create_sheet)

        # 寫入資料
        response = retry_on_error(update_data)
        print(f"✅ 成功寫入特殊客供商記錄，共 {len(rows)-1} 筆")
    except Exception as e:
        print(f"寫入特殊客供商記錄時出錯: {str(e)}")


product_mapping = load_product_code_mapping(sheets_service, product_code_sheet_id)
account_mapping = load_account_mapping(sheets_service, account_query_sheet_id)
statistics_data, special_vendor_dates = collect_statistics_data(files, sheets_service, product_mapping, account_mapping)
write_to_statistics_sheet(sheets_service, statistics_sheet_id, statistics_data)
write_special_vendor_dates(sheets_service, statistics_sheet_id, special_vendor_dates)