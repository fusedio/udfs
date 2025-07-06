@fused.udf(cache_max_age="30m")
def udf(
    path: str,
):
    import requests
    import json

    # Use Fused's built-in signed URL functionality
    signed_url = fused.api.sign_url(path)
    print(signed_url)

    # Create HTML with JavaScript to fetch and display content
    max_chars: int = 100_000
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Text Viewer</title>
        <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
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
                padding: 20px 20px 0 20px;
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
            
            .markdown-content {{
                background: #1a1a1a;
                color: #cccccc;
                padding: 20px 20px 0 20px;
                height: 100vh;
                width: 100vw;
                overflow-y: auto;
                font-size: 15px;
                line-height: 1.6;
            }}
            
            .markdown-content h1, .markdown-content h2, .markdown-content h3, 
            .markdown-content h4, .markdown-content h5, .markdown-content h6 {{
                color: #D1E550;
                margin: 20px 0 10px 0;
            }}
            
            .markdown-content h1 {{ font-size: 2em; }}
            .markdown-content h2 {{ font-size: 1.5em; }}
            .markdown-content h3 {{ font-size: 1.3em; }}
            
            .markdown-content p {{
                margin: 10px 0;
            }}
            
            .markdown-content code {{
                background: #2a2a2a;
                padding: 2px 6px;
                border-radius: 3px;
                font-family: 'JetBrains Mono', monospace;
            }}
            
            .markdown-content pre {{
                background: #2a2a2a;
                padding: 15px;
                border-radius: 5px;
                overflow-x: auto;
                margin: 15px 0;
            }}
            
            .markdown-content pre code {{
                background: none;
                padding: 0;
            }}
            
            .markdown-content blockquote {{
                border-left: 4px solid #D1E550;
                padding-left: 15px;
                margin: 15px 0;
                color: #999;
            }}
            
            .markdown-content ul, .markdown-content ol {{
                margin: 10px 0;
                padding-left: 20px;
            }}
            
            .markdown-content li {{
                margin: 5px 0;
            }}
            
            .markdown-content table {{
                border-collapse: collapse;
                width: 100%;
                margin: 15px 0;
            }}
            
            .markdown-content th, .markdown-content td {{
                border: 1px solid #444;
                padding: 8px 12px;
                text-align: left;
            }}
            
            .markdown-content th {{
                background: #2a2a2a;
                color: #D1E550;
            }}
            
            .markdown-content a {{
                color: #D1E550;
                text-decoration: none;
            }}
            
            .markdown-content a:hover {{
                text-decoration: underline;
            }}
            
            .text-content::-webkit-scrollbar,
            .markdown-content::-webkit-scrollbar {{
                width: 8px;
            }}
            
            .text-content::-webkit-scrollbar-track,
            .markdown-content::-webkit-scrollbar-track {{
                background: #1a1a1a;
            }}
            
            .text-content::-webkit-scrollbar-thumb,
            .markdown-content::-webkit-scrollbar-thumb {{
                background: #D1E550;
                border-radius: 4px;
            }}
            
            .text-content::-webkit-scrollbar-thumb:hover,
            .markdown-content::-webkit-scrollbar-thumb:hover {{
                background: #E8FF59;
            }}
            
            .error {{
                color: #ff6b6b;
                padding: 20px;
                text-align: center;
            }}
            
            .spinner {{
                display: inline-block;
                width: 50px;
                height: 50px;
                border: 3px solid rgba(209, 229, 80, 0.3);
                border-radius: 50%;
                border-top-color: #D1E550;
                animation: spin 1s ease-in-out infinite;
                margin-right: 15px;
            }}
            
            @keyframes spin {{
                to {{ transform: rotate(360deg); }}
            }}
            
            .loading-container {{
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
                height: 100vh;
                gap: 20px;
            }}
            
            .loading-text {{
                font-size: 18px;
                color: #D1E550;
                font-weight: 500;
            }}
            
            .progress-container {{
                width: 300px;
                margin-top: 10px;
            }}
            
            .progress-bar {{
                width: 100%;
                height: 6px;
                background-color: rgba(209, 229, 80, 0.2);
                border-radius: 3px;
                overflow: hidden;
                margin-bottom: 8px;
            }}
            
            .progress-fill {{
                height: 100%;
                background-color: #D1E550;
                border-radius: 3px;
                width: 0%;
                transition: width 0.3s ease;
            }}
            
            .progress-text {{
                font-size: 14px;
                color: #cccccc;
                text-align: center;
            }}
            
            .truncation-message {{
                background: #2a2a2a;
                border: 2px solid #D1E550;
                border-radius: 8px;
                padding: 15px;
                margin: 20px 0;
                color: #D1E550;
                font-weight: bold;
                text-align: center;
                font-size: 16px;
            }}
            
            .truncation-end {{
                background: #2a2a2a;
                border: 1px solid #666;
                border-radius: 5px;
                padding: 0;
                margin: 0;
                color: #D1E550;
                font-style: italic;
                text-align: center;
                font-size: 12px;
            }}
        </style>
    </head>
    <body>
        <div id="content" class="loading-container">
            <div class="spinner"></div>
            <div class="loading-text">Loading content...</div>
            <div class="progress-container">
                <div class="progress-bar">
                    <div class="progress-fill" id="progressFill"></div>
                </div>
                <div class="progress-text" id="progressText">0%</div>
            </div>
        </div>
        
        <script>
            const signedUrl = '{signed_url}';
            const charsPerSection = {max_chars};
            
            // Configure marked.js for better rendering
            marked.setOptions({{
                breaks: true,
                gfm: true,
                headerIds: true,
                mangle: false
            }});
            
            function isMarkdown(text) {{
                // Check for Markdown patterns
                const markdownPatterns = [
                    /^#+\s/,           // Headers
                    /^\*\s/,           // Unordered lists
                    /^\d+\.\s/,        // Ordered lists
                    /^\|.*\|$/,        // Tables
                    /^```/,            // Code blocks
                    /^>/,              // Blockquotes
                    /^\*\*.*\*\*/,     // Bold text
                    /^\*.*\*/,         // Italic text
                    /^\[.*\]\(.*\)/,   // Links
                    /^!\[.*\]\(.*\)/   // Images
                ];
                
                const lines = text.split('\\n');
                const markdownLines = lines.filter(line => 
                    markdownPatterns.some(pattern => pattern.test(line.trim()))
                );
                
                // If more than 10% of lines contain markdown patterns, consider it markdown
                return markdownLines.length > lines.length * 0.1;
            }}
            
            async function loadContent() {{
                try {{
                    // Start conservative progress simulation
                    let progress = 0;
                    const targetProgress = 50;
                    const progressStep = 0.5;
                    
                    const progressInterval = setInterval(() => {{
                        if (progress < targetProgress) {{
                            progress += progressStep;
                            updateProgress(progress);
                        }}
                    }}, 200);
                    
                    const response = await fetch(signedUrl);
                    if (!response.ok) {{
                        clearInterval(progressInterval);
                        throw new Error(`HTTP error! status: ${{response.status}}`);
                    }}
                    
                    // Conservative progression to completion
                    updateProgress(Math.max(progress, 60));
                    await new Promise(resolve => setTimeout(resolve, 400));
                    updateProgress(Math.max(progress, 75));
                    await new Promise(resolve => setTimeout(resolve, 400));
                    
                    const text = await response.text();
                    updateProgress(Math.max(progress, 90));
                    clearInterval(progressInterval);
                    
                    // Small delay to show 100%
                    await new Promise(resolve => setTimeout(resolve, 500));
                    updateProgress(100);
                    
                    const totalChars = text.length;
                    const isMarkdownContent = isMarkdown(text);
                    
                    if (isMarkdownContent) {{
                        // For markdown, render with marked.js
                        let displayContent = text;
                        let truncationMessage = '';
                        
                        if (totalChars > charsPerSection * 2) {{
                            const beginning = text.substring(0, charsPerSection);
                            const ending = text.substring(totalChars - charsPerSection);
                            const middleChars = totalChars - (charsPerSection * 2);
                            
                            displayContent = beginning + 
                                `\\n\\n<div style="color: #D1E550; font-weight: bold; text-align: center; padding: 10px; background: #2a2a2a; border-radius: 5px; margin: 60px 0;">--- CONTENT TRUNCATED (${{middleChars.toLocaleString()}} characters hidden) ---</div>\\n\\n` +
                                ending;
                            truncationMessage = `\\n\\n<div class="truncation-end"></div>`;
                        }}
                        
                        // Render markdown to HTML
                        const renderedMarkdown = marked.parse(displayContent + truncationMessage);
                        
                        document.getElementById('content').innerHTML = `
                            <div class="markdown-content">${{renderedMarkdown}}</div>
                            ${{totalChars > charsPerSection * 2 ? `<div class="truncation-end">Truncated preview. Full size: ${{(totalChars / 1000000).toFixed(1)}}M chars.</div>` : ''}}
                        `;
                    }} else {{
                        // For regular text, display as before
                        let displayContent = text;
                        let truncationMessage = '';
                        
                        if (totalChars > charsPerSection * 2) {{
                            const beginning = text.substring(0, charsPerSection);
                            const ending = text.substring(totalChars - charsPerSection);
                            const middleChars = totalChars - (charsPerSection * 2);
                            
                            displayContent = beginning + 
                                `\\n\\n<div style="color: #D1E550; font-weight: bold; text-align: center; padding: 10px; background: #2a2a2a; border-radius: 5px; margin: 60px 0;">--- CONTENT TRUNCATED (${{middleChars.toLocaleString()}} characters hidden) ---</div>\\n\\n` +
                                ending;
                            truncationMessage = `\\n\\n<div class="truncation-end"></div>`;
                        }}
                        
                        document.getElementById('content').innerHTML = `
                            <div class="text-content">${{displayContent}}${{truncationMessage}}</div>
                            ${{totalChars > charsPerSection * 2 ? `<div class="truncation-end">Truncated preview. Full size: ${{(totalChars / 1000000).toFixed(1)}}M chars.</div>` : ''}}
                        `;
                    }}
                    
                }} catch (error) {{
                    document.getElementById('content').innerHTML = `
                        <div class="error">
                            <h2>Error loading content</h2>
                            <p>${{error.message}}</p>
                            <p>URL: ${{signedUrl}}</p>
                        </div>
                    `;
                }}
            }}
            
            let highestProgress = 0;
            
            function updateProgress(percent) {{
                // Never go backwards - only show the highest percentage reached
                highestProgress = Math.max(highestProgress, percent);
                const progressFill = document.getElementById('progressFill');
                const progressText = document.getElementById('progressText');
                if (progressFill && progressText) {{
                    progressFill.style.width = highestProgress + '%';
                    progressText.textContent = Math.round(highestProgress) + '%';
                }}
            }}
            
            loadContent();
        </script>
    </body>
    </html>
    """

    return html_content
