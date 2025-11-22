import uvicorn
from app.logger import setup_logging

setup_logging()

if __name__ == "__main__":
    uvicorn.run(
        "app.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
