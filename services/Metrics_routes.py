from fastapi import FastAPI, Query, HTTPException
import services.metrics_service as metrics_service

app = FastAPI(title="Metrics API", version="1.0")
# ----------------- METRICS ROUTER -----------------
from fastapi import APIRouter
# ----------------- ROOT -----------------
@app.get("/")
def root():
    return {"message": "Welcome to Metrics API"}



metrics_router = APIRouter(prefix="/metrics", tags=["Metrics"])

@metrics_router.get("/service/{service_name}")
def get_metrics_service(service_name: str, top: int = Query(50, ge=1, le=100)):
    try:
        metrics = metrics_service.get_metrics_by_service(service_name, top)
        return {"service": service_name, "top": top, "metrics": metrics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CosmosDB query failed: {e}")

@metrics_router.get("/latest")
def get_metrics_summary(top: int = Query(50, ge=1, le=100)):
    try:
        metrics = metrics_service.get_metrics_summary(top)
        return {"top": top, "metrics": metrics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CosmosDB query failed: {e}")

@metrics_router.get("/anomalies")
def get_metrics_anomalies(top: int = Query(50, ge=1, le=100)):
    try:
        metrics = metrics_service.get_metrics_anomalies(top)
        return {"top": top, "metrics": metrics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CosmosDB query failed: {e}")
@metrics_router.get("/avg-cpu/{service_name}")
def avg_cpu(service_name: str):
    try:
        data = metrics_service.get_avg_cpu_by_service(service_name)
        return {"service": service_name, "avg_cpu": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@metrics_router.get("/avg-memory/{service_name}")
def avg_memory(service_name: str):
    try:
        data = metrics_service.get_avg_memory_by_service(service_name)
        return {"service": service_name, "avg_memory": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@metrics_router.get("/max-latency/{service_name}")
def max_latency(service_name: str):
    try:
        data = metrics_service.get_max_latency_by_service(service_name)
        return {"service": service_name, "max_latency": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@metrics_router.get("/error-rate/{service_name}")
def error_rate(service_name: str):
    try:
        data = metrics_service.get_error_rate_by_service(service_name)
        return {"service": service_name, "error_rate_percent": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@metrics_router.get("/top-users/{service_name}")
def top_users(service_name: str, top: int = Query(5, ge=1, le=20)):
    try:
        data = metrics_service.get_top_users_by_requests(service_name, top)
        return {"service": service_name, "top_users": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# Include router
app.include_router(metrics_router)
