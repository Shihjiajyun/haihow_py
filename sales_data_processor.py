# 導入必要的函式庫
import pandas as pd
import numpy as np
import datetime
import re
import time
import logging
import os
import glob
from typing import List, Dict, Union, Optional
from pathlib import Path

# 版本資訊
__version__ = '1.0.0'

# 設定基本日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 建立logger實例
logger = logging.getLogger(__name__)

class SalesDataProcessor:
    def __init__(self):
        """初始化銷售數據處理器"""
        self.folder_path = ""
        self.statistics_output_path = ""
        self.account_query_file_path = ""
        self.customer_code_file_path = ""
        self.product_code_file_path = ""
        
        self.product_mapping = {}
        self.account_mapping = {}
        
    def setup_paths(self):
        """設定檔案路徑"""
        print("=== 銷售數據處理器設定 ===")
        
        # 輸入各種檔案路徑
        self.folder_path = input("請輸入要掃描試算表的資料夾路徑：").strip().strip('"').strip("'")
        self.statistics_output_path = input("請輸入統計資料輸出檔案路徑 (例: output_statistics.xlsx)：").strip().strip('"').strip("'")
        self.account_query_file_path = input("請輸入查詢挂帳試算表檔案路徑：").strip().strip('"').strip("'")
        self.customer_code_file_path = input("請輸入查詢客戶供應商代號和傳票類別試算表檔案路徑：").strip().strip('"').strip("'")
        self.product_code_file_path = input("請輸入產品代號試算表檔案路徑：").strip().strip('"').strip("'")
        
        # 驗證路徑
        if not os.path.exists(self.folder_path):
            raise ValueError(f"資料夾路徑不存在: {self.folder_path}")
        
        print("所有路徑設定完成")
        
    def get_excel_files(self) -> List[Dict[str, str]]:
        """取得資料夾內的所有 Excel 檔案"""
        try:
            # 支援多種 Excel 格式
            excel_patterns = ['*.xlsx', '*.xls', '*.xlsm']
            files = []
            
            for pattern in excel_patterns:
                full_pattern = os.path.join(self.folder_path, pattern)
                matched_files = glob.glob(full_pattern)
                
                for file_path in matched_files:
                    file_name = os.path.basename(file_path)
                    files.append({
                        'name': file_name,
                        'path': file_path
                    })
            
            if not files:
                raise ValueError("指定的資料夾內沒有 Excel 試算表")
            
            print(f"資料夾內 Excel 試算表數量: {len(files)}")
            print("試算表列表：")
            for file in files:
                print(f"- {file['name']}")
                
            return files
            
        except Exception as e:
            print(f"掃描資料夾時發生錯誤: {str(e)}")
            raise

    def normalize_product_name(self, name):
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

    def load_product_code_mapping(self):
        """載入產品代號對照表"""
        try:
            # 先嘗試讀取 Sheet2
            df = pd.DataFrame()
            
            # 檢查檔案是否為 HTML 格式
            try:
                with open(self.product_code_file_path, 'rb') as f:
                    first_bytes = f.read(20)
                    if b'<html' in first_bytes.lower() or b'<!doctype' in first_bytes.lower():
                        # HTML 檔案，讀取所有表格並找到 Sheet2 相關的
                        tables = pd.read_html(self.product_code_file_path, encoding='utf-8')
                        if len(tables) >= 2:
                            df = tables[1]  # 第二個表格可能對應 Sheet2
                        elif len(tables) >= 1:
                            df = tables[0]  # 只有一個表格就用第一個
                    else:
                        # 真正的 Excel 檔案，讀取 Sheet2
                        file_ext = os.path.splitext(self.product_code_file_path)[1].lower()
                        if file_ext == '.xls':
                            df = pd.read_excel(self.product_code_file_path, sheet_name='Sheet2', engine='xlrd')
                        else:
                            df = pd.read_excel(self.product_code_file_path, sheet_name='Sheet2', engine='openpyxl')
            except Exception as e:
                print(f"讀取 Sheet2 失敗，嘗試讀取第一個工作表: {str(e)}")
                df = self.read_excel_sheet(self.product_code_file_path)
            
            mapping = {}
            
            print(f"產品代號表欄位: {list(df.columns)}")
            print(f"產品代號表資料筆數: {len(df)}")
            
            # 根據您的截圖，B欄是代號，C欄是品名
            if len(df.columns) >= 3:
                for idx, row in df.iterrows():
                    # B欄(索引1)是代號，C欄(索引2)是品名
                    if pd.notna(row.iloc[1]) and pd.notna(row.iloc[2]):
                        code = str(row.iloc[1]).strip()  # B欄代號
                        name = str(row.iloc[2]).strip()  # C欄品名
                        normalized_name = self.normalize_product_name(name)
                        mapping[normalized_name] = code
                        
                        # 除錯：顯示前幾筆資料
                        if idx < 5:
                            print(f"  {name} -> {normalized_name} -> {code}")
            
            self.product_mapping = mapping
            print(f"載入產品代號對照表完成，共 {len(mapping)} 筆")
            
            # 特別檢查服務費
            service_fee_key = self.normalize_product_name("[服務費]")
            if service_fee_key in mapping:
                print(f"找到服務費對應代號: {mapping[service_fee_key]}")
            else:
                print(f"未找到服務費，標準化後的鍵值: '{service_fee_key}'")
                print("對照表中的前10個鍵值:")
                for i, key in enumerate(list(mapping.keys())[:10]):
                    print(f"  '{key}'")
            
            return mapping
            
        except Exception as e:
            print(f"載入產品代號表失敗: {str(e)}")
            self.product_mapping = {}
            return {}

    def load_account_mapping(self):
        """載入掛帳傳票對照表"""
        try:
            df = pd.read_excel(self.account_query_file_path)
            mapping = {}
            
            # 假設 B 欄是帳號，J 欄是傳票類別
            if len(df.columns) >= 10:
                for _, row in df.iterrows():
                    if pd.notna(row.iloc[1]) and pd.notna(row.iloc[9]):  # B, J 欄
                        account_id = str(row.iloc[1]).strip().zfill(6)  # 補滿六位
                        voucher_type = str(row.iloc[9]).strip()
                        mapping[account_id] = voucher_type
            
            self.account_mapping = mapping
            print(f"載入掛帳傳票對照表完成，共 {len(mapping)} 筆")
            return mapping
            
        except Exception as e:
            print(f"載入挂帳傳票對照表失敗: {str(e)}")
            self.account_mapping = {}
            return {}

    def read_excel_sheet(self, file_path: str) -> pd.DataFrame:
        """讀取 Excel 檔案的第一個工作表"""
        try:
            # 首先檢查檔案是否為 HTML 格式
            with open(file_path, 'rb') as f:
                first_bytes = f.read(20)
                if b'<html' in first_bytes.lower() or b'<!doctype' in first_bytes.lower():
                    # 這是 HTML 檔案，使用 pandas 的 HTML 讀取功能
                    try:
                        tables = pd.read_html(file_path, encoding='utf-8')
                        if tables:
                            return tables[0]  # 返回第一個表格
                        else:
                            print(f"HTML 檔案中沒有找到表格: {file_path}")
                            return pd.DataFrame()
                    except Exception as html_error:
                        print(f"讀取 HTML 格式失敗 {file_path}: {str(html_error)}")
                        return pd.DataFrame()
            
            # 根據副檔名選擇適當的引擎讀取真正的 Excel 檔案
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.xls':
                # 舊版 Excel 格式使用 xlrd 引擎
                df = pd.read_excel(file_path, sheet_name=0, engine='xlrd')
            elif file_ext in ['.xlsx', '.xlsm']:
                # 新版 Excel 格式使用 openpyxl 引擎
                df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl')
            else:
                # 預設嘗試自動偵測
                df = pd.read_excel(file_path, sheet_name=0)
            
            return df
            
        except Exception as e:
            print(f"讀取檔案失敗 {file_path}: {str(e)}")
            return pd.DataFrame()

    def extract_filtered_column_from_sheets(self, files, target_column_name="品　種", time_column_name="時間"):
        """從試算表中提取並篩選指定欄位的資料"""
        skip_keywords = {
            self.normalize_product_name(k) for k in [
                "現金", "MASTER", "VISA", "挂帳", "Visa", "AE", "Master", "挂帳",
                "jcb", "匯款", "訂金", "銀聯"
            ]
        }
        
        for file in files:
            file_path = file['path']
            file_name = file['name']
            
            try:
                df = self.read_excel_sheet(file_path)
                
                if df.empty:
                    print(f"[{file_name}] 沒有資料")
                    continue
                
                if target_column_name not in df.columns or time_column_name not in df.columns:
                    print(f"[{file_name}] 缺少 '{target_column_name}' 或 '{time_column_name}' 欄位")
                    continue
                
                print(f"\n[{file_name}] 的篩選後「{target_column_name}」欄位資料如下：")
                
                for _, row in df.iterrows():
                    if pd.notna(row[target_column_name]) and pd.notna(row[time_column_name]):
                        variety = str(row[target_column_name]).strip()
                        time_val = str(row[time_column_name]).strip()
                        
                        if self.normalize_product_name(variety) in skip_keywords:
                            continue
                        if time_val == "" or time_val == "nan":
                            continue
                        
                        print(f"- {variety}")
                        
            except Exception as e:
                print(f"讀取 [{file_name}] 時發生錯誤: {str(e)}")

    def collect_statistics_data(self, files, target_column_name="品　種", time_column_name="時間"):
        """收集統計資料"""
        skip_keywords = {
            self.normalize_product_name(k) for k in [
                "現金", "MASTER", "VISA", "挂帳", "Visa", "AE", "Master", "挂帳",
                "jcb", "匯款", "訂金", "銀聯"
            ]
        }
        
        output_rows = []
        special_vendor_dates = []
        special_vendor_codes = {'52', '53', '54', '55'}
        
        def process_vendor_code(code):
            """處理客供商代號：除了特殊代號外，都補滿六位數"""
            code = str(code).strip()
            if code in special_vendor_codes:
                return code
            return code.zfill(6)
        
        def get_tax_code(voucher_type):
            """根據傳票類別決定稅別"""
            if voucher_type in ['S994', 'S997']:
                return "2"
            return "6"
        
        for file in files:
            file_path = file['path']
            file_name = file['name']
            first_entry_written = False
            
            try:
                # 從檔名提取日期 (例：23210225002 -> 0225)
                m = re.search(r'^.{4}(\d{4})', file_name)
                if m:
                    month_day = m.group(1)
                    month = month_day[:2]
                    day = month_day[2:]
                    spreadsheet_date = f"114/{month}/{day}"
                else:
                    spreadsheet_date = ""
            except:
                spreadsheet_date = ""
            
            try:
                df = self.read_excel_sheet(file_path)
                
                if df.empty:
                    continue
                
                if target_column_name not in df.columns or time_column_name not in df.columns:
                    continue
                
                # 取得各欄位的索引
                required_columns = {
                    'product': target_column_name,
                    'time': time_column_name,
                    'unit_price': '單價',
                    'category': '類別',
                    'quantity': '數量',
                    'amount': '金額',
                    'invoice': '發票',
                    'reason': '贈送原因'
                }
                
                # 檢查欄位是否存在
                available_columns = {}
                for key, col_name in required_columns.items():
                    available_columns[key] = col_name if col_name in df.columns else None
                
                # 分析付款方式
                all_product_raws = []
                for _, row in df.iterrows():
                    if pd.notna(row.get(target_column_name, "")):
                        all_product_raws.append(str(row[target_column_name]).strip())
                
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
                default_vendor_code = "000999"
                vendor_code_lookup = None
                
                if "挂帳" in all_methods_detected and available_columns['category']:
                    for _, row in df.iterrows():
                        if pd.notna(row.get(target_column_name, "")) and pd.notna(row.get(available_columns['category'], "")):
                            if str(row[target_column_name]).strip() in ["挂帳", "挂帳"]:
                                vendor_code_raw = str(row[available_columns['category']]).strip()
                                default_vendor_code = process_vendor_code(vendor_code_raw)
                                vendor_code_lookup = default_vendor_code
                                if vendor_code_raw in special_vendor_codes:
                                    special_vendor_dates.append({
                                        'date': spreadsheet_date,
                                        'vendor_code': vendor_code_raw,
                                        'spreadsheet_name': file_name
                                    })
                                break
                
                # 取得總金額
                total_amount_value = ""
                if available_columns['amount']:
                    for _, row in df.iterrows():
                        if pd.notna(row.get(target_column_name, "")) and str(row[target_column_name]).strip() == "結帳 小計":
                            if pd.notna(row.get(available_columns['amount'], "")):
                                total_amount_value = str(row[available_columns['amount']]).strip()
                            break
                
                # 處理每一筆商品資料
                first_entry = True
                for _, row in df.iterrows():
                    if not pd.notna(row.get(target_column_name, "")) or not pd.notna(row.get(time_column_name, "")):
                        continue
                    
                    product_raw = str(row[target_column_name]).strip()
                    time_raw = str(row[time_column_name]).strip()
                    
                    if self.normalize_product_name(product_raw) in skip_keywords or time_raw == "" or time_raw == "nan":
                        continue
                    
                    # 檢查單價和贈送原因
                    current_vendor_code = default_vendor_code
                    is_pr_item = False
                    
                    # 檢查是否為公關品
                    if available_columns['reason'] and pd.notna(row.get(available_columns['reason'], "")):
                        is_pr_item = str(row[available_columns['reason']]).strip() == "公關品"
                    
                    is_service_fee = self.normalize_product_name(product_raw) == self.normalize_product_name("[服務費]")
                    
                    # 檢查單價
                    unit_price = 0.0
                    if available_columns['unit_price'] and pd.notna(row.get(available_columns['unit_price'], "")):
                        try:
                            unit_price_str = str(row[available_columns['unit_price']]).replace(",", "").strip()
                            if unit_price_str == "" or unit_price_str == "nan":
                                if is_service_fee:
                                    unit_price = 0.0
                                else:
                                    continue
                            else:
                                unit_price = float(unit_price_str)
                                
                                if unit_price == 0:
                                    if not is_pr_item:
                                        if is_service_fee:
                                            unit_price = 0.0
                                        else:
                                            continue
                                    else:
                                        current_vendor_code = "000995"
                                else:
                                    if is_pr_item:
                                        current_vendor_code = "000995"
                                    elif payment_method == "挂帳":
                                        current_vendor_code = default_vendor_code
                                    else:
                                        current_vendor_code = "000999"
                        except ValueError:
                            if is_service_fee:
                                unit_price = 0.0
                            else:
                                continue
                    
                    # 取得其他欄位資料
                    product_key = self.normalize_product_name(product_raw)
                    product_code = self.product_mapping.get(product_key, f"未查到此商品({product_raw})")
                    
                    if product_raw.strip() == "[服務費]":
                        quantity = "1"
                    elif available_columns['quantity'] and pd.notna(row.get(available_columns['quantity'], "")):
                        quantity = str(row[available_columns['quantity']]).strip()
                    else:
                        quantity = ""
                    
                    amount = ""
                    if available_columns['amount'] and pd.notna(row.get(available_columns['amount'], "")):
                        amount = str(row[available_columns['amount']]).strip().replace(",", "")
                    
                    # 設定傳票類別
                    if payment_method == "多種":
                        voucher_type = "S994"
                    elif payment_method == "挂帳" and vendor_code_lookup:
                        voucher_type = self.account_mapping.get(vendor_code_lookup, "未查到")
                    else:
                        voucher_type = {"現金": "S998", "信用卡": "S997", "匯款": "S996"}.get(payment_method, "S996")
                    
                    # 檢查是否為公關品並設定相應的傳票類別
                    if is_pr_item:
                        voucher_type = ""
                    
                    # 根據傳票類別和發票狀況設定稅別
                    if voucher_type == 'S998':  # 現金付款
                        # 檢查是否有發票號碼
                        has_invoice = bool(invoice_number_for_output.strip() if not first_entry else False)
                        tax_code = "2" if has_invoice else "6"
                    else:
                        tax_code = get_tax_code(voucher_type)
                    
                    # 設定現金/刷卡金額
                    if payment_method == "現金":
                        cash_amount = amount
                        card_amount = ""
                    elif payment_method == "信用卡":
                        cash_amount = ""
                        card_amount = amount
                    else:
                        cash_amount = ""
                        card_amount = ""
                    
                    # 處理總金額和發票相關資料
                    total_amount = ""
                    invoice_number_for_output = ""
                    has_invoice_for_output = False
                    
                    if first_entry:
                        # 發票號碼處理
                        all_invoice_numbers = []
                        if available_columns['invoice']:
                            for _, invoice_row in df.iterrows():
                                if pd.notna(invoice_row.get(available_columns['invoice'], "")):
                                    invoice_str = str(invoice_row[available_columns['invoice']]).strip()
                                    m = re.search(r"發票號:(\w+)", invoice_str)
                                    if "發票金額:0" in invoice_str:
                                        continue
                                    elif m:
                                        all_invoice_numbers.append(m.group(1))
                        
                        has_invoice_for_output = bool(all_invoice_numbers)
                        
                        # 格式化總金額
                        try:
                            amount_numeric = round(float(total_amount_value.replace(",", "")))
                            total_amount = str(amount_numeric).zfill(8)
                        except:
                            total_amount = ""
                        
                        # 發票和備註處理
                        if payment_method == "多種":
                            invoice_number_for_output = ""
                            remarks_m250 = "_".join(all_invoice_numbers) if all_invoice_numbers else ""
                            has_invoice_for_output = False
                        else:
                            invoice_number_for_output = all_invoice_numbers[0] if all_invoice_numbers else ""
                            remarks_m250 = ""
                            has_invoice_for_output = invoice_number_for_output != ""
                    
                    first_entry = False
                    
                    # 開發票判斷
                    has_invoice = False
                    if available_columns['invoice'] and pd.notna(row.get(available_columns['invoice'], "")):
                        invoice_str = str(row[available_columns['invoice']]).strip()
                        has_invoice = not invoice_str.startswith("發票金額:0")
                    
                    # 未稅邏輯計算
                    untaxed_price = ""
                    untaxed_amount = ""
                    tax_amount = ""
                    
                    if unit_price > 0:
                        untaxed_price = str(round(unit_price))
                    
                    if amount:
                        try:
                            amt = float(amount.replace(",", ""))
                            untaxed_amount = str(round(amt))
                            tax_amount = str(round(amt - amt / 1.05))
                        except:
                            untaxed_amount = ""
                            tax_amount = ""
                    
                    # 備註和發票號碼（只在第一筆寫入）
                    remarks_to_write = remarks_m250 if not first_entry_written else ""
                    invoice_number_to_write = invoice_number_for_output if not first_entry_written else ""
                    
                    # 計算總稅額
                    total_tax = ""
                    if not first_entry_written and available_columns['invoice']:
                        invoice_amounts = []
                        for _, tax_row in df.iterrows():
                            if pd.notna(tax_row.get(available_columns['invoice'], "")):
                                invoice_str = str(tax_row[available_columns['invoice']]).strip()
                                m_amt = re.search(r"發票金額:(\d+)", invoice_str)
                                if m_amt:
                                    invoice_amounts.append(int(m_amt.group(1)))
                        
                        try:
                            total_invoice_amt = sum(invoice_amounts)
                            total_tax = str(round(total_invoice_amt - total_invoice_amt / 1.05))
                        except:
                            total_tax = ""
                    
                    # 處理銷貨單號 - 去除副檔名
                    sales_order_number = os.path.splitext(file_name)[0]
                    
                    # 處理數量 - 轉為整數
                    formatted_quantity = ""
                    if quantity:
                        try:
                            qty_float = float(str(quantity).replace(",", ""))
                            formatted_quantity = str(int(qty_float))
                        except (ValueError, TypeError):
                            formatted_quantity = quantity
                    
                    # 組織輸出資料
                    output_rows.append([
                        sales_order_number,       # B 銷貨單號（檔名，不含副檔名）
                        product_code,             # E 產品代號
                        spreadsheet_date,         # G 銷貨日期
                        payment_method,           # R 付款方式
                        current_vendor_code,      # C 客供商代號
                        voucher_type,             # Z 傳票類別
                        formatted_quantity,       # F 數量（整數）
                        tax_code,                 # S 稅別
                        cash_amount,              # T 付現金額
                        card_amount,              # U 刷卡金額
                        total_amount,             # V 含稅總金額
                        untaxed_price,            # AA 未稅單價
                        untaxed_amount,           # AB 未稅金額
                        tax_amount,               # AC 稅額
                        invoice_number_to_write,  # AK 發票號碼
                        remarks_to_write,         # AW 備註
                        total_tax if not first_entry_written else "",  # W 欄（總稅額）
                    ])
                    
                    first_entry_written = True
                    
            except Exception as e:
                print(f"[{file_name}] 錯誤: {str(e)}")
        
        return output_rows, special_vendor_dates

    def write_to_excel(self, statistics_data, special_vendor_dates):
        """將統計資料寫入 Excel 檔案"""
        try:
            # 檢查並創建輸出資料夾
            output_dir = os.path.dirname(self.statistics_output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"已創建輸出資料夾: {output_dir}")
            
            # 準備統計資料
            aligned_data = []
            for row in statistics_data:
                row_out = [""] * 49  # 足夠的欄位數
                row_out[1] = row[0]   # B欄：銷貨單號
                row_out[2] = row[4]   # C欄：客供商代號
                row_out[4] = row[1]   # E欄：產品代號
                row_out[5] = row[6]   # F欄：數量
                row_out[6] = row[2]   # G欄：銷貨日期
                row_out[18] = row[7]  # S欄：稅別
                row_out[21] = row[10] # V欄：總含稅金額
                row_out[28] = row[13] # AC欄：稅額
                row_out[36] = row[14] # AK欄：發票號碼
                row_out[48] = row[15] # AW欄：備註M250
                row_out[25] = row[5]  # Z欄：傳票類別
                row_out[26] = row[11] # AA欄：未稅單價
                row_out[27] = row[12] # AB欄：未稅金額
                row_out[22] = row[16] # W欄：總稅額
                
                aligned_data.append(row_out)
            
            # 創建 Excel 檔案
            with pd.ExcelWriter(self.statistics_output_path, engine='openpyxl') as writer:
                # 寫入統計資料 - 從第二列開始寫入
                df_stats = pd.DataFrame(aligned_data)
                df_stats.to_excel(writer, sheet_name='統計資料', index=False, header=False, startrow=1)
                
                # 寫入特殊客供商記錄
                if special_vendor_dates:
                    special_data = []
                    for entry in special_vendor_dates:
                        special_data.append([
                            entry['date'],
                            entry['vendor_code'],
                            entry['spreadsheet_name']
                        ])
                    
                    df_special = pd.DataFrame(special_data, columns=['日期', '客供商代號', '銷貨單號'])
                    df_special.to_excel(writer, sheet_name='特殊客供商記錄', index=False)
            
            print(f"✅ 成功寫入統計資料，共 {len(statistics_data)} 筆")
            print(f"✅ 檔案已儲存至: {self.statistics_output_path}")
            
        except PermissionError:
            print(f"❌ 權限錯誤：無法寫入檔案 {self.statistics_output_path}")
            print("可能的原因：")
            print("1. 檔案正在 Excel 中開啟，請關閉檔案後重試")
            print("2. 沒有寫入該資料夾的權限")
            print("3. 檔案被其他程式鎖定")
            print("\n建議解決方案：")
            print("1. 關閉所有開啟該檔案的程式（如 Excel）")
            print("2. 確認資料夾路徑存在且有寫入權限")
            print("3. 嘗試使用不同的檔案名稱")
        except FileNotFoundError:
            print(f"❌ 檔案路徑錯誤：找不到目標資料夾")
            print(f"路徑: {self.statistics_output_path}")
            print("請確認資料夾路徑是否正確")
        except Exception as e:
            print(f"❌ 寫入 Excel 檔案時發生未預期錯誤: {str(e)}")
            print("請檢查檔案路徑和權限設定")

    def run(self):
        """執行主程式"""
        try:
            print("=== 銷售數據處理器 ===")
            
            # 設定檔案路徑
            self.setup_paths()
            
            # 載入對照表
            print("\n載入對照表...")
            self.load_product_code_mapping()
            self.load_account_mapping()
            
            # 取得 Excel 檔案列表
            print("\n掃描 Excel 檔案...")
            files = self.get_excel_files()
            
            # 收集統計資料
            print("\n處理銷售數據...")
            statistics_data, special_vendor_dates = self.collect_statistics_data(files)
            
            # 寫入結果
            print("\n寫入結果...")
            self.write_to_excel(statistics_data, special_vendor_dates)
            
            print("\n=== 處理完成 ===")
            
        except Exception as e:
            print(f"執行過程中發生錯誤: {str(e)}")
            raise

def main():
    """主函式"""
    processor = SalesDataProcessor()
    processor.run()

if __name__ == "__main__":
    main()
