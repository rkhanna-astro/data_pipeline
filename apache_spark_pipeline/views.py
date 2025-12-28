from django.http import JsonResponse
from .services.sync_service import run_sync
from .services.tigergraph_singleton import TIGERGRAPH_CLIENT


def sync_batch(request):
    run_sync("batch")
    return JsonResponse({"status": "Batch sync completed"})

def sync_micro(request):
    last_ts = request.GET.get("last_ts")
    run_sync("micro", last_ts)
    return JsonResponse({"status": "Micro-batch sync completed"})

def users(request):
    tg = TIGERGRAPH_CLIENT
    return JsonResponse(tg.fetch_vertices("User"), safe=False)

def products(request):
    tg = TIGERGRAPH_CLIENT
    return JsonResponse(tg.fetch_vertices("Product"), safe=False)

def purchases(request):
    tg = TIGERGRAPH_CLIENT
    return JsonResponse(tg.fetch_edges("PURCHASED"), safe=False)
