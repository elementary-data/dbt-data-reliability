# dbt-data-reliability dbt Package Tests

## Usage

1. Start a Postgres instance on your machine using Docker:

```shell
docker-compose up -d
```

2. Add the following profile to your `profiles.yml`:

```shell
elementary_tests:
  target: postgres
  outputs:
    postgres:
      type: postgres
      host: 127.0.0.1
      port: 5432
      user: admin
      password: admin
      dbname: postgres
      schema: edr
      threads: 32
```

3. Install tests' requirements.

```shell
pip install -r requirements.txt
```

4. Install elementary-data

`elementary-data` is required for testing. Install specific version if latest doesn't fit your needs.

```shell
pip install elementary-data
```

5. Run the tests.

```shell
pytest tests -vvv -n8
```

### Web Interface

You can browse the database by visiting http://localhost:5433 in your browser.
The credentials are:

- **Email**: admin@admin.com
- **Password**: admin

It is also recommended to set the search path to your Elementary schema by running: `SET search_path = edr_elementary`.  
That will allow you to do `SELECT * FROM dbt_models` rather than `SELECT * FROM edr_elementary.dbt_models`.
