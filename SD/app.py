# app.py
from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello, this is a secure Flask app!"

if __name__ == "__main__":
    app.run(ssl_context=('cert.pem', 'key.pem'), debug=True)
