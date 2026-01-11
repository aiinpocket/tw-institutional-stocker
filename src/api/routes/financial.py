"""Financial data routes - dividends and EPS from TWSE/TPEx OpenAPI."""
import requests
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from datetime import datetime

from src.api.dependencies import get_db
from src.common.models import Stock, MonthlyRevenue

router = APIRouter()

# Disable SSL warnings for Taiwan government APIs
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
REQUEST_TIMEOUT = 30


def convert_roc_date(roc_date: str) -> str:
    """Convert ROC date format (1140122 or 114/01/22) to ISO date (2025-01-22)."""
    if not roc_date:
        return None
    # Remove slashes
    roc_date = roc_date.replace("/", "")
    if len(roc_date) == 7:
        year = int(roc_date[:3]) + 1911
        month = roc_date[3:5]
        day = roc_date[5:7]
        return f"{year}-{month}-{day}"
    return None


@router.get("/dividends/{code}")
def get_dividend_info(
    code: str,
    limit: int = Query(10, description="Number of records to return", le=50),
):
    """Get dividend information for a stock from TWSE/TPEx."""
    dividends = []

    # Try TWSE first (upcoming dividends)
    try:
        url = "https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL"
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
        if resp.status_code == 200:
            data = resp.json()
            for item in data:
                if item.get("Code") == code:
                    dividends.append({
                        "ex_date": convert_roc_date(item.get("Date")),
                        "type": "除息" if item.get("Exdividend") == "息" else "除權",
                        "cash_dividend": float(item.get("CashDividend")) if item.get("CashDividend") else None,
                        "stock_dividend_ratio": float(item.get("StockDividendRatio")) if item.get("StockDividendRatio") else None,
                        "source": "TWSE"
                    })
    except Exception as e:
        pass

    # Try TWSE historical
    try:
        today = datetime.now()
        url = f"https://www.twse.com.tw/rwd/zh/exRight/TWT49U?date={today.strftime('%Y%m%d')}&selectType=ALL&response=json"
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
        if resp.status_code == 200:
            result = resp.json()
            if result.get("stat") == "OK" and result.get("data"):
                for row in result["data"]:
                    if len(row) >= 8 and row[1] == code:
                        dividends.append({
                            "ex_date": convert_roc_date(row[0].replace("年", "").replace("月", "").replace("日", "")),
                            "type": row[6],  # 權/息
                            "pre_close": float(row[3]) if row[3] else None,
                            "ref_price": float(row[4]) if row[4] else None,
                            "dividend_value": float(row[5]) if row[5] else None,
                            "source": "TWSE_historical"
                        })
    except Exception as e:
        pass

    # Try TPEx
    try:
        url = f"https://www.tpex.org.tw/web/stock/exright/dailyquo/exDailyQ_result.php?l=zh-tw&o=json&d={datetime.now().strftime('%Y/%m/%d')}"
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
        if resp.status_code == 200:
            result = resp.json()
            if result.get("tables") and len(result["tables"]) > 0:
                table = result["tables"][0]
                for row in table.get("data", []):
                    if len(row) >= 10 and row[1].strip() == code:
                        dividends.append({
                            "ex_date": convert_roc_date(row[0]),
                            "type": row[8],
                            "pre_close": float(row[2]) if row[2] else None,
                            "ref_price": float(row[3]) if row[3] else None,
                            "cash_dividend": float(row[13]) if len(row) > 13 and row[13] else None,
                            "source": "TPEx"
                        })
    except Exception as e:
        pass

    return {
        "code": code,
        "dividends": dividends[:limit]
    }


@router.get("/eps/{code}")
def get_eps_info(code: str):
    """Get EPS information for a stock from TWSE/TPEx."""
    eps_data = []
    stock_name = None
    industry = None

    # Try TWSE
    try:
        url = "https://openapi.twse.com.tw/v1/opendata/t187ap14_L"
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
        if resp.status_code == 200:
            data = resp.json()
            for item in data:
                if item.get("公司代號") == code:
                    stock_name = item.get("公司名稱")
                    industry = item.get("產業別")
                    eps_data.append({
                        "year": int(item.get("年度", 0)) + 1911 if item.get("年度") else None,
                        "quarter": int(item.get("季別", 0)) if item.get("季別") else None,
                        "eps": float(item.get("基本每股盈餘(元)", 0)) if item.get("基本每股盈餘(元)") else None,
                        "revenue": float(item.get("營業收入", 0)) if item.get("營業收入") else None,
                        "operating_profit": float(item.get("營業利益", 0)) if item.get("營業利益") else None,
                        "net_income": float(item.get("稅後淨利", 0)) if item.get("稅後淨利") else None,
                        "source": "TWSE"
                    })
    except Exception as e:
        pass

    # Try TPEx if no data from TWSE
    if not eps_data:
        try:
            url = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O"
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
            if resp.status_code == 200:
                data = resp.json()
                for item in data:
                    if item.get("SecuritiesCompanyCode") == code:
                        stock_name = item.get("CompanyName")
                        industry = item.get("產業別")
                        eps_data.append({
                            "year": int(item.get("Year", 0)) + 1911 if item.get("Year") else None,
                            "quarter": int(item.get("季別", 0)) if item.get("季別") else None,
                            "eps": float(item.get("基本每股盈餘", 0)) if item.get("基本每股盈餘") else None,
                            "revenue": float(item.get("營業收入", 0)) if item.get("營業收入") else None,
                            "operating_profit": float(item.get("營業利益", 0)) if item.get("營業利益") else None,
                            "net_income": float(item.get("稅後淨利", 0)) if item.get("稅後淨利") else None,
                            "source": "TPEx"
                        })
        except Exception as e:
            pass

    # Sort by year and quarter (most recent first)
    eps_data.sort(key=lambda x: (x.get("year") or 0, x.get("quarter") or 0), reverse=True)

    return {
        "code": code,
        "name": stock_name,
        "industry": industry,
        "eps_history": eps_data
    }


@router.get("/revenue/{code}")
def get_revenue_history(
    code: str,
    limit: int = Query(12, description="Number of months to return", le=36),
    db: Session = Depends(get_db)
):
    """Get monthly revenue history from database."""
    stock = db.query(Stock).filter(Stock.code == code).first()
    if not stock:
        return {"code": code, "revenue_history": []}

    revenues = (
        db.query(MonthlyRevenue)
        .filter(MonthlyRevenue.stock_id == stock.id)
        .order_by(MonthlyRevenue.year.desc(), MonthlyRevenue.month.desc())
        .limit(limit)
        .all()
    )

    return {
        "code": code,
        "name": stock.name,
        "revenue_history": [
            {
                "year": r.year,
                "month": r.month,
                "revenue": int(r.revenue) if r.revenue else None,
                "mom_change": float(r.mom_change) if r.mom_change else None,
                "yoy_change": float(r.yoy_change) if r.yoy_change else None,
                "cumulative_revenue": int(r.cumulative_revenue) if r.cumulative_revenue else None,
            }
            for r in revenues
        ]
    }


@router.get("/summary/{code}")
def get_financial_summary(code: str, db: Session = Depends(get_db)):
    """Get combined financial summary for a stock."""
    dividend_info = get_dividend_info(code, limit=5)
    eps_info = get_eps_info(code)
    revenue_info = get_revenue_history(code, limit=12, db=db)

    return {
        "code": code,
        "name": eps_info.get("name") or revenue_info.get("name"),
        "industry": eps_info.get("industry"),
        "dividends": dividend_info.get("dividends", []),
        "eps_history": eps_info.get("eps_history", []),
        "revenue_history": revenue_info.get("revenue_history", [])
    }
