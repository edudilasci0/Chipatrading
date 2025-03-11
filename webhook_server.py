from fastapi import FastAPI, Request
import uvicorn

app = FastAPI()

@app.post("/webhook")
async def webhook_endpoint(request: Request):
    data = await request.json()
    print("Webhook recibido:", data)
    # Aquí podrías llamar a alguna función del bot para procesar el evento, por ejemplo:
    # await signal_logic.process_helius_event(data)
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
