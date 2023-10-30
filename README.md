# Atmospheric Pipeline

This spark pipeline will:

1. Connect to a source database
2. Listen to the `create_or_update_record` trigger
3. For each change, run the relevant quality assertions, erroring when data is incorrect
4. Write correct data to a destination database

## Configuration

| Variable | Description | Default |
|---|---|---|
| `SOURCE_DATABASE` | The database to read from, as a postgres connection string | `` |
| `DEST_DATABASE` | The database to write to, as a postgres connection string | `` |
| `SPARK_MASTER` | The address of the Spark Master | `spark://127.0.1.1:7077` |

### Database Configuration

To configure the necessary triggers on postgres, simply run:

```sql
CREATE OR REPLACE FUNCTION process_record() RETURNS TRIGGER as $process_record$
BEGIN
    PERFORM pg_notify('create_or_update_record', '{"tbl":"' || TG_TABLE_NAME || '","id":"' || COALESCE(NEW.id, 0) || '"}');
    RETURN NEW;
END;
$process_record$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER precipitation_trigger
AFTER INSERT OR UPDATE OR DELETE ON precipitation FOR EACH ROW
EXECUTE PROCEDURE process_record();
```

This will configure a trigger which sends the payload:

```json
{"tbl":"precipitation","id":"5"}
```

Which is used to configure spark jobs.

## Development

This project uses [black](https://github.com/psf/black) to format code. Any code which has not been formatted with black will fail CI
