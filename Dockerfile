FROM python:3.10
WORKDIR /backend
COPY requirements.txt /backend
RUN pip install --upgrade pip -r requirements.txt
COPY . /backend
CMD ["python", "main.py"]