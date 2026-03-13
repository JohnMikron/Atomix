"""
FastAPI + Atomix STM Integration Example

This example demonstrates how to use Atomix STM to manage shared state (inventory)
safely and efficiently in a high-concurrency web environment.

Scenario: A high-performance flash sale where multiple users try to buy limited stock.
STM ensures that we never oversell, even with massive concurrent requests,
leveraging Python 3.13's No-GIL capabilities if available.
"""

from fastapi import FastAPI, HTTPException
from atomix_stm import Atom, dosync, retry
import uvicorn
import asyncio

app = FastAPI(title="Atomix STM FastAPI Demo")

# Shared state: Dictionary of products and their stock
# We wrap the entire inventory in an Atom, or individual items.
# For high concurrency on specific items, individual Atoms are better.
inventory = {
    "laptop": Atom(10),
    "phone": Atom(50),
    "watch": Atom(5)
}

# Shared state: Total sales counter
total_sales = Atom(0)

@app.get("/stock/{item_id}")
async def get_stock(item_id: str):
    if item_id not in inventory:
        raise HTTPException(status_code=404, detail="Item not found")
    
    # deref() is safe outside of dosync() for simple reads
    return {"item": item_id, "stock": inventory[item_id].deref()}

@app.post("/purchase/{item_id}")
async def purchase_item(item_id: str):
    if item_id not in inventory:
        raise HTTPException(status_code=404, detail="Item not found")
    
    stock_atom = inventory[item_id]
    
    def tx():
        current_stock = stock_atom.deref()
        if current_stock <= 0:
            raise ValueError("Out of stock")
        
        # Decrement stock and increment total sales atomically
        stock_atom.reset(current_stock - 1)
        total_sales.swap(lambda x: x + 1)
        
        return {"status": "success", "remaining": current_stock - 1}

    try:
        # dosync() handles all the retry logic and conflict resolution automatically
        result = await asyncio.to_thread(dosync, tx)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/sales-stats")
async def get_stats():
    return {
        "total_sales": total_sales.deref(),
        "inventory_snapshot": {k: v.deref() for k, v in inventory.items()}
    }

if __name__ == "__main__":
    print("Starting Atomix STM FastAPI Demo...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
