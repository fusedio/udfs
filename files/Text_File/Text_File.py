@fused.udf
def udf(path: str='s3://fused-magic/robots.txt', as_html=True):
    if as_html:
        import requests
        import json
        
        # Use Fused's built-in signed URL functionality
        signed_url = fused.api.sign_url(path)
        print(signed_url)
        
        # Fetch the text content from the signed URL
        response = requests.get(signed_url)
        text_content = response.text
        print(f"Content length: {len(text_content)}")
        print(f"Content preview: {text_content[:100]}")
        
        # Create simple full-screen HTML with just the text content
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Text Viewer</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: 'JetBrains Mono', 'Fira Code', 'Monaco', 'Consolas', monospace;
                    background: #1a1a1a;
                    color: #cccccc;
                    height: 100vh;
                    overflow: hidden;
                }}
                
                .text-content {{
                    background: #1a1a1a;
                    color: #cccccc;
                    padding: 20px;
                    height: 100vh;
                    width: 100vw;
                    overflow-y: auto;
                    font-size: 15px;
                    white-space: pre-wrap;
                    word-wrap: break-word;
                    border: none;
                    outline: none;
                    resize: none;
                    line-height: 1.6;
                }}
                
                .text-content::-webkit-scrollbar {{
                    width: 8px;
                }}
                
                .text-content::-webkit-scrollbar-track {{
                    background: #1a1a1a;
                }}
                
                .text-content::-webkit-scrollbar-thumb {{
                    background: #D1E550;
                    border-radius: 4px;
                }}
                
                .text-content::-webkit-scrollbar-thumb:hover {{
                    background: #E8FF59;
                }}
            </style>
        </head>
        <body>
            <div class="text-content">{text_content}</div>
        </body>
        </html>
        """
        
        return html_content
    
    else:
        import fsspec
        with fsspec.open(path, 'r') as f:
            print(f.read())
