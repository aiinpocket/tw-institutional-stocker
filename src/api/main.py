"""FastAPI application for Taiwan Stock Institutional Tracker."""
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, PlainTextResponse, Response
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.routes import stocks, institutional, prices, rankings, brokers, strategy, analysis, system, industry, ai_analysis, margin, revenue, financial
from src.api.dependencies import get_db

TAIPEI_TZ = ZoneInfo("Asia/Taipei")

# Cache-Control headers for HTML pages (no-cache forces revalidation)
HTML_CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0"
}

app = FastAPI(
    title="Taiwan Stock Institutional Tracker API",
    description="API for tracking institutional investor holdings in Taiwan stocks",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(stocks.router, prefix="/api/v1/stocks", tags=["Stocks"])
app.include_router(institutional.router, prefix="/api/v1/institutional", tags=["Institutional"])
app.include_router(prices.router, prefix="/api/v1/prices", tags=["Prices"])
app.include_router(rankings.router, prefix="/api/v1/rankings", tags=["Rankings"])
app.include_router(brokers.router, prefix="/api/v1/brokers", tags=["Brokers"])
app.include_router(strategy.router, prefix="/api/v1/strategy", tags=["Strategy"])
app.include_router(analysis.router, prefix="/api/v1/analysis", tags=["Analysis"])
app.include_router(system.router, prefix="/api/v1/system", tags=["System"])
app.include_router(industry.router, prefix="/api/v1/industry", tags=["Industry"])
app.include_router(ai_analysis.router, prefix="/api/v1/ai", tags=["AI Analysis"])
app.include_router(margin.router, prefix="/api/v1/margin", tags=["Margin Trading"])
app.include_router(revenue.router, prefix="/api/v1/revenue", tags=["Revenue"])
app.include_router(financial.router, prefix="/api/v1/financial", tags=["Financial"])

# Serve static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/dashboard")
def dashboard():
    """Serve the strategy dashboard."""
    html_path = os.path.join(static_dir, "dashboard.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, headers=HTML_CACHE_HEADERS)
    return {"error": "Dashboard not found"}


@app.get("/stock/{stock_code}")
def stock_detail(stock_code: str):
    """Serve the stock detail page."""
    html_path = os.path.join(static_dir, "stock.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, headers=HTML_CACHE_HEADERS)
    return {"error": "Stock page not found"}


@app.get("/industry")
def industry_page():
    """Serve the industry heatmap page."""
    html_path = os.path.join(static_dir, "industry.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, headers=HTML_CACHE_HEADERS)
    return {"error": "Industry page not found"}


@app.get("/ai")
def ai_page():
    """Serve the AI analysis page."""
    html_path = os.path.join(static_dir, "ai.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, headers=HTML_CACHE_HEADERS)
    return {"error": "AI page not found"}


@app.get("/rankings")
def rankings_page():
    """Serve the institutional rankings page."""
    html_path = os.path.join(static_dir, "rankings.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, headers=HTML_CACHE_HEADERS)
    return {"error": "Rankings page not found"}


@app.get("/brokers")
def brokers_page():
    """Serve the broker tracking page."""
    html_path = os.path.join(static_dir, "brokers.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, headers=HTML_CACHE_HEADERS)
    return {"error": "Brokers page not found"}


@app.get("/live")
def live_page():
    """Serve the live dashboard page."""
    html_path = os.path.join(static_dir, "live.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, headers=HTML_CACHE_HEADERS)
    return {"error": "Live page not found"}


@app.get("/margin")
def margin_page():
    """Serve the margin trading page."""
    html_path = os.path.join(static_dir, "margin.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, headers=HTML_CACHE_HEADERS)
    return {"error": "Margin page not found"}


@app.get("/revenue")
def revenue_page():
    """Serve the revenue tracking page."""
    html_path = os.path.join(static_dir, "revenue.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, headers=HTML_CACHE_HEADERS)
    return {"error": "Revenue page not found"}


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    """Serve robots.txt for search engine crawlers."""
    return """# Taiwan Stock Institutional Tracker - robots.txt
# https://stock-tw.aiinpocket.com

User-agent: *
Allow: /
Allow: /dashboard
Allow: /live
Allow: /rankings
Allow: /industry
Allow: /brokers
Allow: /ai
Allow: /margin
Allow: /revenue
Allow: /stock/

# Disallow API and documentation
Disallow: /api/
Disallow: /docs
Disallow: /redoc
Disallow: /static/

# Crawl-delay for politeness
Crawl-delay: 1

# Sitemap location
Sitemap: https://stock-tw.aiinpocket.com/sitemap.xml
"""


@app.get("/sitemap.xml")
def sitemap_xml(db: Session = Depends(get_db)):
    """Serve dynamic sitemap.xml for search engines including popular stocks."""
    today = datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d")

    # Static pages
    sitemap_entries = [
        ("https://stock-tw.aiinpocket.com/dashboard", "daily", "1.0"),
        ("https://stock-tw.aiinpocket.com/live", "daily", "0.9"),
        ("https://stock-tw.aiinpocket.com/rankings", "daily", "0.8"),
        ("https://stock-tw.aiinpocket.com/industry", "daily", "0.8"),
        ("https://stock-tw.aiinpocket.com/brokers", "daily", "0.7"),
        ("https://stock-tw.aiinpocket.com/ai", "daily", "0.7"),
        ("https://stock-tw.aiinpocket.com/margin", "daily", "0.6"),
        ("https://stock-tw.aiinpocket.com/revenue", "monthly", "0.6"),
    ]

    # Get popular stocks (top 100 by recent trading volume)
    try:
        stock_query = text("""
            SELECT DISTINCT s.code
            FROM stocks s
            JOIN stock_prices sp ON s.id = sp.stock_id
            WHERE s.is_active = true
            AND sp.trade_date >= CURRENT_DATE - INTERVAL '7 days'
            AND sp.volume > 0
            ORDER BY s.code
            LIMIT 200
        """)
        stocks_result = db.execute(stock_query).fetchall()
        stock_codes = [row[0] for row in stocks_result]
    except Exception:
        # Fallback to common Taiwan stocks
        stock_codes = ["2330", "2317", "2454", "2308", "2881", "2882", "2412", "3008", "2303", "1301"]

    # Build sitemap XML
    sitemap_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    sitemap_parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

    # Add static pages
    for url, freq, priority in sitemap_entries:
        sitemap_parts.append(f"""    <url>
        <loc>{url}</loc>
        <lastmod>{today}</lastmod>
        <changefreq>{freq}</changefreq>
        <priority>{priority}</priority>
    </url>""")

    # Add stock pages
    for code in stock_codes:
        sitemap_parts.append(f"""    <url>
        <loc>https://stock-tw.aiinpocket.com/stock/{code}</loc>
        <lastmod>{today}</lastmod>
        <changefreq>daily</changefreq>
        <priority>0.5</priority>
    </url>""")

    sitemap_parts.append('</urlset>')
    sitemap = '\n'.join(sitemap_parts)

    return Response(content=sitemap, media_type="application/xml")


@app.get("/oauth/callback")
def oauth_callback(code: str = None, error: str = None, error_description: str = None):
    """OAuth callback endpoint for Threads authorization."""
    if error:
        return Response(
            content=f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>OAuth 錯誤</title></head>
<body style="font-family: sans-serif; padding: 40px; text-align: center;">
<h1>授權失敗</h1>
<p>錯誤：{error}</p>
<p>{error_description or ''}</p>
</body></html>""",
            media_type="text/html"
        )

    if code:
        return Response(
            content=f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>OAuth 成功</title></head>
<body style="font-family: sans-serif; padding: 40px; text-align: center;">
<h1>授權成功！</h1>
<p>你的授權碼 (code)：</p>
<textarea style="width: 100%; max-width: 600px; height: 100px; font-size: 14px;" readonly onclick="this.select()">{code}</textarea>
<p style="color: #666; margin-top: 20px;">請複製上面的授權碼，貼給開發者換取 Access Token</p>
</body></html>""",
            media_type="text/html"
        )

    return Response(
        content="""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>OAuth Callback</title></head>
<body style="font-family: sans-serif; padding: 40px; text-align: center;">
<h1>OAuth Callback</h1>
<p>未收到授權碼</p>
</body></html>""",
        media_type="text/html"
    )


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "tw-stocker-api"}


@app.get("/")
def root():
    """Redirect to dashboard."""
    return RedirectResponse(url="/dashboard")
