@fused.udf
def udf(path: str):
    import requests
    import json
    
    # Use Fused's built-in signed URL functionality
    signed_url = fused.api.sign_url(path)
    print(signed_url)
    
    # Create HTML with JavaScript to fetch and display content
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
            
            .markdown-content {{
                background: #1a1a1a;
                color: #cccccc;
                padding: 20px;
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
            const maxChars = 10000;
            const charsPerSection = 4000; // Show 4000 chars from beginning and 4000 from end
            
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
                    // Start smooth progress simulation - more aggressive
                    let progress = 0;
                    const targetProgress = 85;
                    const progressStep = 5; // Increased from 2 to 5 for more aggressive progress
                    
                    const progressInterval = setInterval(() => {{
                        if (progress < targetProgress) {{
                            progress += progressStep;
                            updateProgress(progress);
                        }}
                    }}, 50); // Reduced from 100ms to 50ms for faster updates
                    
                    const response = await fetch(signedUrl);
                    if (!response.ok) {{
                        clearInterval(progressInterval);
                        throw new Error(`HTTP error! status: ${{response.status}}`);
                    }}
                    
                    // Smooth progression to completion - ensure no decrease
                    const currentProgress = Math.max(progress, 85); // Never go below current progress
                    updateProgress(currentProgress);
                    await new Promise(resolve => setTimeout(resolve, 100)); // Reduced delay
                    updateProgress(Math.max(currentProgress + 5, 90));
                    await new Promise(resolve => setTimeout(resolve, 100)); // Reduced delay
                    
                    const text = await response.text();
                    updateProgress(100);
                    clearInterval(progressInterval);
                    
                    // Small delay to show 100%
                    await new Promise(resolve => setTimeout(resolve, 200)); // Reduced delay
                    
                    const totalChars = text.length;
                    const isMarkdownContent = isMarkdown(text);
                    
                    if (isMarkdownContent) {{
                        // For markdown, render with marked.js
                        let displayContent = text;
                        let truncationMessage = '';
                        
                        if (totalChars > maxChars) {{
                            const beginning = text.substring(0, charsPerSection);
                            const ending = text.substring(totalChars - charsPerSection);
                            const middleChars = totalChars - (charsPerSection * 2);
                            
                            displayContent = beginning + 
                                `\\n\\n--- CONTENT TRUNCATED (${{middleChars.toLocaleString()}} characters hidden) ---\\n` +
                                ending;
                            truncationMessage = `\\n\\n--- END OF DISPLAY ---\\nTotal file size: ${{totalChars.toLocaleString()}} characters`;
                        }}
                        
                        // Render markdown to HTML
                        const renderedMarkdown = marked.parse(displayContent + truncationMessage);
                        
                        document.getElementById('content').innerHTML = `
                            <div class="markdown-content">${{renderedMarkdown}}</div>
                        `;
                    }} else {{
                        // For regular text, display as before
                        let displayContent = text;
                        let truncationMessage = '';
                        
                        if (totalChars > maxChars) {{
                            const beginning = text.substring(0, charsPerSection);
                            const ending = text.substring(totalChars - charsPerSection);
                            const middleChars = totalChars - (charsPerSection * 2);
                            
                            displayContent = beginning + 
                                `\\n\\n--- CONTENT TRUNCATED (${{middleChars.toLocaleString()}} characters hidden) ---\\n` +
                                ending;
                            truncationMessage = `\\n\\n--- END OF DISPLAY ---\\nTotal file size: ${{totalChars.toLocaleString()}} characters`;
                        }}
                        
                        document.getElementById('content').innerHTML = `
                            <div class="text-content">${{displayContent}}${{truncationMessage}}</div>
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
            
            function updateProgress(percent) {{
                const progressFill = document.getElementById('progressFill');
                const progressText = document.getElementById('progressText');
                if (progressFill && progressText) {{
                    // Ensure progress never decreases
                    const currentWidth = parseFloat(progressFill.style.width) || 0;
                    const newWidth = Math.max(currentWidth, percent);
                    progressFill.style.width = newWidth + '%';
                    progressText.textContent = Math.round(newWidth) + '%';
                }}
            }}
            
            loadContent();
        </script>
    </body>
    </html>
    """
      
    return html_content
