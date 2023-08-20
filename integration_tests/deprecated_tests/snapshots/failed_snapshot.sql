{% snapshot failed_snapshot() %}

    {{
        config(
            target_schema="snapshots",
            unique_key="unique_id",
            strategy="timestamp",
            updated_at="generated_at",
        )
    }}
    select failed_snapshot
{% endsnapshot %}
