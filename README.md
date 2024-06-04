# iggy-python-client

This repository provides a Python library powered by Rust using `pyo3`. It also utilizes Docker for server deployment.

## Quick Start

### 1. Run the Server with Docker:

Ensure you have Docker installed on your system. Then, execute the following command:

```
docker run --rm -p 8080:8080 -p 3000:3000 -p 8090:8090 iggyrs/iggy:0.2.23
```


This command runs the server and maps the specified ports to your local machine.

### 2. Install Maturin:

Maturin is used for building Rust binaries for Python. Install it with:

```
pip install maturin
```

### 3. Build and Install the pyo3 Library:

Navigate to your library's root directory and execute:

```
maturin develop
```


This will build the Rust library and make it available for Python.

### 4. Start the Producer:

Navigate to the `python_examples` directory and run:

```
python producer.py
```

### 5. Start the Consumer:

Still in the `python_examples` directory, run:

```
python consumer.py
```
