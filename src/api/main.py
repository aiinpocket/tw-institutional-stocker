"""FastAPI application for Taiwan Stock Institutional Tracker."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse
import os

from src.api.routes import stocks, institutional, prices, rankings, brokers, strategy, analysis, system, industry, ai_analysis

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

# Serve static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/dashboard")
def dashboard():
    """Serve the strategy dashboard."""
    html_path = os.path.join(static_dir, "dashboard.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    return {"error": "Dashboard not found"}


@app.get("/stock/{stock_code}")
def stock_detail(stock_code: str):
    """Serve the stock detail page."""
    html_path = os.path.join(static_dir, "stock.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    return {"error": "Stock page not found"}


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "tw-stocker-api"}


@app.get("/")
def root():
    """Redirect to dashboard."""
    return RedirectResponse(url="/dashboard")
