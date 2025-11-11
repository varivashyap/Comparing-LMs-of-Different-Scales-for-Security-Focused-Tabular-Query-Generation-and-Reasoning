import numpy as np
import pandas as pd
import os
from openai import AzureOpenAI
import json

def load_schema(schema_path):
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    with open(schema_path, 'r') as file:
        schema = json.load(file)
    table_name = schema.get("table_name")
    table_description = schema.get("table_description")
    return table_name, table_description, schema

def generate_synthetic_data(table1, table2, table3, endpoint, deployment, api_key, api_version):

    endpoint = endpoint
    deployment = deployment
    api_key = api_key
    api_version = api_version
    
    client = AzureOpenAI(
        api_version=api_version,
        azure_endpoint=endpoint,
        api_key=api_key,
    )
    
    user_prompt = f"""
    You are an expert security data analyst and query generator.
    You are provided with 3 JSON files containing the schemas of 3 different tables:
    The schemas contain the columns of the table, their data types, and a description of what each column represents.
    
    Table 1:
    Name: {table1[0]}
    Schema: {table1[2]}
    
    Table 2:
    Name: {table2[0]}
    Schema: {table2[2]}
    
    Table 3:
    Name: {table3[0]}
    Schema: {table3[2]}

    
    Using this information, generate 25 interesting, complex, high-quality and non-trivial security investigation questions that can be answered by combining and analyzing data from these tables.
    Ensure that the questions are complex enough to require all 3 tables for a comprehensive answer. The questions should be multi-step.
    Ensure that the questions make sense in the context of security investigations.
    
    Your response should have the following format:
    Question:
    Purpose (what does the question aim to achieve):
    Reasoning (how to answer the question using all 3 tables):
        
    Example:
    Find all events where potentially malicious script execution that may indicate attempts to download payloads, bypass security controls, or establish persistence are observed . Identify each device where this activity was observed and analyze if there is any other anomalous behavior with the device. Correlate with Device Compliance to identify the compliance state of the device. Generate an attack chain if a true privilege escalation attempt was identified. Provide a detailed report of the analysis.
    """
    
    response = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are a data analyst assistant.",
            },
            {
                "role": "user",
                "content": user_prompt,
            }
        ],
        max_tokens=10000,
        temperature=0.7,
        top_p=0.8,
        frequency_penalty=0.1,
        presence_penalty=0.1,
        model=deployment
    )

    return response.choices[0].message.content

def main():
    
    openai_config_path = "C:\\Users\\polad\\Desktop\\Logs\\Varivashya_Poladi\\Synthetic Complex Query Generation\\openai_config.json"
    
    if not os.path.exists(openai_config_path):
        raise FileNotFoundError(f"OpenAI configuration file not found: {openai_config_path}")
    with open(openai_config_path, 'r') as file:
        openai_config = json.load(file)
    
    api_version = openai_config.get("api_version")
    azure_endpoint = openai_config.get("endpoint")
    api_key = openai_config.get("api_key")
    deployment = openai_config.get("deployment", "gpt-4.1")
        
    data_config_path = "C:\\Users\\polad\\Desktop\\Logs\\Varivashya_Poladi\\Synthetic Complex Query Generation\\data_config.json"
    
    if not os.path.exists(data_config_path):
        raise FileNotFoundError(f"Data configuration file not found: {data_config_path}")
    
    with open(data_config_path, 'r') as file:
        data_config = json.load(file)
    
    path1 = data_config.get("table1_schema_path")
    path2 = data_config.get("table2_schema_path")
    path3 = data_config.get("table3_schema_path")
    
    output_path = data_config.get("output_path")
    
    # Load the schemas.
    
    def load_table(path):
        a, b, c = load_schema(path)
        return [a, b, c]
    
    table1 = load_table(path1)
    table2 = load_table(path2)
    table3 = load_table(path3)
    
    # Generate synthetic data.
    questions = generate_synthetic_data(table1, table2, table3, azure_endpoint, deployment, api_key, api_version)
    
    # Save the generated questions to a file.
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    output_file = os.path.join(output_path, "generated_questions.txt")
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(questions)
    print(f"Generated questions saved to {output_file}")
    
if __name__ == "__main__":
    main()