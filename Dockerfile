FROM python:3.13-slim

# 1. Εγκατάσταση απαραίτητων εργαλείων και PostgreSQL Client 18
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    ca-certificates \
    gnupg \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg \
#    && curl -fsSL https://postgresql.org | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg \
    && echo "deb http://postgresql.org trixie-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y postgresql-client-18 \
    && rm -rf /var/lib/apt/lists/*

# 2. Ορισμός φακέλου εργασίας
WORKDIR /app

# 3. Clone του repo
RUN git clone https://github.com/atsob/Personal_Finance.git /tmp/repo \
    && mv /tmp/repo/* /app/ \
    && mv /tmp/repo/.[!.]* /app/ 2>/dev/null || true \
    && rm -rf /tmp/repo

# 4. Εγκατάσταση dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Ρυθμίσεις περιβάλλοντος
ARG DB_PASSWORD
ENV DB_PASSWORD=${DB_PASSWORD}

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]
