FROM python:3.12-slim
WORKDIR /app
COPY app.py page.html snapshot.json ./
ENV PORT=8080 DATA_DIR=/data SCAN_INTERVAL=180
EXPOSE 8080
CMD ["python","app.py"]
