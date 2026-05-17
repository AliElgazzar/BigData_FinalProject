import os
from flask import Flask, render_template_string
from pyhive import hive

app = Flask(__name__)

HIVE_HOST = os.getenv("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "bigdata_project")


def run_query(query):
    try:
        conn = hive.Connection(
            host=HIVE_HOST,
            port=HIVE_PORT,
            database=HIVE_DATABASE,
            username="hive"
        )
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return columns, rows, None
    except Exception as error:
        return [], [], str(error)


def table_html(title, columns, rows):
    html = f"<h2>{title}</h2>"
    if not rows:
        html += "<p class='waiting'>Waiting for data...</p>"
        return html

    html += "<table><thead><tr>"
    for col in columns:
        html += f"<th>{col}</th>"
    html += "</tr></thead><tbody>"

    for row in rows:
        html += "<tr>"
        for value in row:
            html += f"<td>{value}</td>"
        html += "</tr>"

    html += "</tbody></table>"
    return html


@app.route("/")
def dashboard():
    latest_cols, latest_rows, latest_err = run_query("""
        SELECT server_name, language, project_type, type, title, editor_user, bot, event_time
        FROM wikimedia_enriched_events
        ORDER BY event_time DESC
        LIMIT 25
    """)

    server_cols, server_rows, server_err = run_query("""
        SELECT server_name, COUNT(*) AS total_changes
        FROM wikimedia_enriched_events
        GROUP BY server_name
        ORDER BY total_changes DESC
        LIMIT 10
    """)

    bot_cols, bot_rows, bot_err = run_query("""
        SELECT bot_label, SUM(event_count) AS total_events
        FROM wikimedia_bot_summary
        GROUP BY bot_label
    """)

    window_cols, window_rows, window_err = run_query("""
        SELECT window_start, window_end, server_name, language, project_type, change_count, processed_time
        FROM wikimedia_window_summary ORDER BY processed_time DESC
        LIMIT 25
    """)

    errors = [e for e in [latest_err, server_err, bot_err, window_err] if e]

    page = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Real-Time Wikimedia Analytics</title>
        <meta http-equiv="refresh" content="5">
        <style>
            body { font-family: Arial, sans-serif; background: #f5f7fb; margin: 30px; color: #222; }
            h1 { color: #1f3b57; }
            .card { background: white; padding: 20px; margin-bottom: 25px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
            table { width: 100%; border-collapse: collapse; font-size: 14px; }
            th { background: #1f3b57; color: white; padding: 8px; text-align: left; }
            td { padding: 8px; border-bottom: 1px solid #ddd; }
            .waiting { color: #777; font-style: italic; }
            .error { background: #ffe3e3; color: #8a1f1f; padding: 12px; border-radius: 8px; margin-bottom: 10px; }
            .subtitle { color: #555; margin-bottom: 25px; }
        </style>
    </head>
    <body>
        <h1>Real-Time Wikimedia Change Analytics Dashboard</h1>
        <p class="subtitle">Wikimedia Stream &rarr; Kafka &rarr; Spark &rarr; Hive &rarr; Dashboard</p>

        {% if errors %}
            <div class="card">
                <h2>Connection Status</h2>
                {% for error in errors %}
                    <div class="error">{{ error }}</div>
                {% endfor %}
            </div>
        {% endif %}

        <div class="card">{{ latest_table|safe }}</div>
        <div class="card">{{ server_table|safe }}</div>
        <div class="card">{{ bot_table|safe }}</div>
        <div class="card">{{ window_table|safe }}</div>
    </body>
    </html>
    """

    return render_template_string(
        page,
        errors=errors,
        latest_table=table_html("Latest Events", latest_cols, latest_rows),
        server_table=table_html("Top Servers by Change Count", server_cols, server_rows),
        bot_table=table_html("Bot vs Human Summary", bot_cols, bot_rows),
        window_table=table_html("One-Minute Window Summary", window_cols, window_rows)
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8501)

