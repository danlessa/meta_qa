def generate_documentation(metadata):
    html = metadata.reset_index().set_index(["table_catalog",
                                      "table_schema",
                                      "table_name",
                                      "table_title",
                                      "table_description_text",
                                      "table_ignore",
                                      "column_name",
                                      "column_title",
                                      "column_data_type",
                                      "column_description_text",
                                      "column_assert"]).to_html(bold_rows=False, header=False, classes="pandas_table")

    html_string = """<html>
      <head><title>HTML Pandas Dataframe with CSS</title></head>
      <link rel="stylesheet" type="text/css" href="style.css"/>
      <body>
      <div class='table-wrapper'>
        {table}
    </div>
      </body>
    </html>"""


    with open('db_doc.html', 'w') as f:
        f.write(html_string.format(table=html))


def generate_tests_report(tests_data):
    pass
