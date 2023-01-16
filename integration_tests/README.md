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

3. Run the tests.

```shell
python run_e2e_tests.py
```

### Extras

You can browse the database by visiting http://localhost:5050 in your browser.
The credentials are:

- **Email**: admin@admin.com
- **Password**: admin
