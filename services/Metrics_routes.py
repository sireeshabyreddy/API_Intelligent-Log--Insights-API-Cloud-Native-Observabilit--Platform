from fastapi import FastAPI, Query, HTTPException, APIRouter
from services.metrics_service import (

    get_metrics_summary,
    get_metrics_anomalies,
    get_avg_cpu_by_service,
    get_avg_memory_by_service,
    get_max_latency_by_service,
    get_error_rate_by_service,
)

app = FastAPI(title="Metrics API", version="1.0")

# ----------------- METRICS ROUTER -----------------
metrics_router = APIRouter(prefix="/metrics", tags=["Metrics"])



@metrics_router.get("/latest")
def get_metrics_summary_route(top: int = Query(50, ge=1, le=100)):
    try:
        metrics = get_metrics_summary(top)
        return {"top": top, "metrics": metrics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CosmosDB query failed: {e}")


# ---------------------------------------------------
# GET ANOMALIES
# ---------------------------------------------------
@metrics_router.get("/anomalies")
def get_metrics_anomalies_route(top: int = Query(50, ge=1, le=100)):
    try:
        metrics = get_metrics_anomalies(top)
        return {"top": top, "metrics": metrics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CosmosDB query failed: {e}")


# ---------------------------------------------------
# AVG CPU
# ---------------------------------------------------
@metrics_router.get("/avg-cpu/{service_name}")
def avg_cpu(service_name: str):
    try:
        data = get_avg_cpu_by_service(service_name)
        return {"service": service_name, "avg_cpu": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------
# AVG MEMORY
# ---------------------------------------------------
@metrics_router.get("/avg-memory/{service_name}")
def avg_memory(service_name: str):
    try:
        data = get_avg_memory_by_service(service_name)
        return {"service": service_name, "avg_memory": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------
# MAX LATENCY
# ---------------------------------------------------
@metrics_router.get("/max-latency/{service_name}")
def max_latency(service_name: str):
    try:
        data = get_max_latency_by_service(service_name)
        return {"service": service_name, "max_latency": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------
# ERROR RATE
# ---------------------------------------------------
@metrics_router.get("/error-rate/{service_name}")
def error_rate(service_name: str):
    try:
        data = get_error_rate_by_service(service_name)
        return {"service": service_name, "error_rate_percent": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------
# HEALTH — NOW ADDED INTO METRICS ROUTER
# ---------------------------------------------------
@metrics_router.get("/health/{service_name}")
def health(service_name: str):
    return {
        "service": service_name,
        "avg_cpu": get_avg_cpu_by_service(service_name),
        "avg_memory": get_avg_memory_by_service(service_name),
        "max_latency_ms": get_max_latency_by_service(service_name),
        "error_rate_percent": (
            get_error_rate_by_service(service_name)[0]
            if get_error_rate_by_service(service_name)
            else 0
        ),
        "recent_anomalies": get_metrics_anomalies(5),
    }


# Register router
app.include_router(metrics_router)
