from dash import Dash, html, dcc, callback, Output, Input, State, dash_table, no_update
import polars as pl
import base64
import io

app = Dash()

pq_to_csv_layout = [
    html.H2(children='Convert parquet to csv'),
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select File')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        multiple=False
    ),
    html.Div(id='output-data-upload')
]

api_to_csv_layout = [
    html.H2(children='Convert API to CSV'),
    dcc.Input(id='api-url', type='url', placeholder='API URL...', style={'width': '100%'}),
    html.B(children='Separator'),
    dcc.Dropdown(id='csv-separator-dd', options=[',', '|'], value=',', clearable=False, multi=False, style={'width': '25%'}),
    html.B(children='Quoting'),
    dcc.Dropdown(id='csv-quoting-dd', options=['No', 'Yes'], value='No', clearable=False, multi=False, style={'width': '25%'}),
    html.Button(id='convert-btn', children='Convert'),
    html.Div(id='output-data-api')
]

app.layout = [
    html.H1(children='Convert to CSV', style={'textAlign': 'center'}),
    dcc.Tabs(children=[dcc.Tab(children=pq_to_csv_layout, label='Parquet to CSV'), dcc.Tab(children=api_to_csv_layout, label='API to CSV')]),
    dcc.Download(id='download-csv')
]

@callback(
    Output('output-data-upload', 'children'),
    Output('download-csv', 'data', allow_duplicate=True),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename'),
    State('upload-data', 'last_modified'),
    prevent_initial_call=True
)
def upload_files(contents, filename, date):
    return parse_content(contents, filename, date) if contents else [no_update, no_update]

@callback(
    Output('output-data-api', 'children'),
    Output('download-csv', 'data', allow_duplicate=True),
    Input('convert-btn', 'n_clicks'),
    State('api-url', 'value'),
    State('csv-separator-dd', 'value'),
    State('csv-quoting-dd', 'value'),
    prevent_initial_call=True
)
def convert_api(n_clicks, url, sep, quote):
    if n_clicks and url:
        try:
            df = pl.read_csv(url, skip_rows=0, separator=sep, infer_schema_length=0).with_columns(pl.all().cast(pl.Utf8))
            quote_style = 'always' if quote == 'Yes' else None
            csv_data = df.write_csv(separator=sep, quote_style=quote_style)
            return html.Div(children=[
                dash_table.DataTable(
                    df.to_dicts(),
                    [{'name': i, 'id': i} for i in df.columns]
                )
            ]),
            dict(content=csv_data, filename='api-data.csv')
        except Exception as e:
            print(e)
            return html.Div(['There was an error processing the API.']), no_update
    else:
        return no_update, no_update

def parse_content(contents, filename, date):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        if 'parquet' in filename:
            file = io.BytesIO(decoded)
            df = pl.read_parquet(file)
        else:
            return html.Div(['Please upload a parquet file to convert.']), no_update
    except Exception as e:
        print(e)
        return html.Div(['There was an error processing this file.']), no_update
    
    csv_data = df.write_csv()
    return html.Div(children=[
        html.H5(filename),
        dash_table.DataTable(
            df.to_dicts(),
            [{'name': i, 'id': i} for i in df.columns]
        ),
        html.Hr(),
        html.Div('Raw Content'),
        html.Pre(contents[0:200] + '...', style={'whiteSpace': 'pre-wrap', 'wordBreak': 'break-all'})
    ]),
    dict(content=csv_data, filename=filename + '.csv')

if __name__ == '__main__':
    app.run(debug=True)

