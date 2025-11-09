# services/logs_router.py
from fastapi import APIRouter, HTTPException, Query
import services.cosmos_service as cosmos_service

router = APIRouter(
    prefix="/logs",
    tags=["Logs"]
)

@router.get("/service/{service_name}")
def get_logs_by_service(service_name: str, top: int = Query(50, ge=1, le=100)):
    """Fetch logs for a specific service."""
    try:
        logs = cosmos_service.query_logs_by_service(service_name, top)
        return {"service": service_name, "top": top, "logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CosmosDB query failed: {e}")

@router.get("/level/{log_level}")
def get_logs_by_level(log_level: str, top: int = Query(50, ge=1, le=100)):
    """Fetch logs filtered by log level (error/warn/info)."""
    try:
        logs = cosmos_service.query_logs_by_level(log_level, top)
        return {"level": log_level, "top": top, "logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CosmosDB query failed: {e}")

@router.get("/anomalies")
def get_anomalous_logs(top: int = Query(50, ge=1, le=100)):
    """Fetch logs marked as anomalies."""
    try:
        logs = cosmos_service.query_anomalous_logs(top)
        return {"top": top, "logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CosmosDB query failed: {e}")
