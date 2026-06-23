FROM python:3.10-slim

WORKDIR /app

# تسطيب حزم النظام الأساسية المطلوبة لـ tgcrypto لتسريع حسابات تليجرام
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# الصورة جاهزة، أوامر التشغيل الفعلية هنقسمها جوه الـ Compose