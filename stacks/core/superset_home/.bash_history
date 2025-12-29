ping
apt-get update
apt-get install ping
sudo apt-get install ping
python -c "import psycopg2; print('psycopg2 OK')"
db upgrade
ps aux | grep gunicorn
superset run -p 8088
curl -v http://localhost:8088/
ss -tulnp | grep 8088
python
python -c "import psycopg2; print('psycopg2 OK')"
# 1) See which Python Superset uses
/app/.venv/bin/python -c "import sys; print(sys.executable)"
# 2) Install psycopg2-binary INTO THAT VENV
/app/.venv/bin/pip install --no-cache-dir psycopg2-binary
# 3) Now test import using same Python
/app/.venv/bin/python -c "import psycopg2; print('psycopg2 OK')"
/app/.venv/bin/python -c "import sys; print(sys.executable)"
/app/.venv/bin/pip install --no-cache-dir psycopg2-binary
./app/.venv/bin/pip install --no-cache-dir psycopg2-binary
cd /app/.venv/bin
ls -lrt
pip
pip install psycopg2-binary
python -c import psycopg2
python
/app/.venv/bin/python -m pip install --no-cache-dir psycopg2-binary
/app/.venv/bin/python -m ensurepip --upgrade
