@fused.udf
def udf():
    """
    Simple PDF table extractor interface.
    Enter a PDF URL, click the button, and it opens the extraction API in a new tab
    which directly downloads a CSV file.
    """
    
    # Show the input UI
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    
    # Print the URLs for debugging
    original_working_url = "https://unstable.fused.io/server/v1/realtime-shared/fsh_2cYag1UM0aM0c4HALo8Z3A/run/file"
    current_url = "https://www.fused.io/server/v1/realtime-shared/fsh_5FVJ7x6aOnF92Zd0zOI5IF/"
    
    print("=== URL DEBUGGING ===")
    print(f"Original working URL: {original_working_url}")
    print(f"Current URL in code: {current_url}")
    print("URL difference: The current URL is missing '/run/file' at the end and has a different UDF ID")

    html_content = f"""
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #E5FF44 0%, #f0ff66 100%); padding: 30px; border-radius: 12px; margin-bottom: 25px; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                <h2 style="margin: 0 0 10px 0; color: #333333; font-size: 28px; font-weight: 700;">PDF Table to CSV 📋</h2>
                <p style="margin: 0; color: #333333; font-size: 16px; opacity: 0.8;">Extract table data from any PDF and download as CSV</p>
            </div>
            
            <div style="background: white; padding: 25px; border-radius: 12px; box-shadow: 0 2px 15px rgba(0,0,0,0.08); margin-bottom: 20px;">
                <label for="pdfUrl" style="display: block; margin-bottom: 8px; font-weight: 600; color: #333333; font-size: 14px;">PDF URL</label>
                <input type="text" id="pdfUrl" style="width: 100%; padding: 14px; margin-bottom: 20px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 14px; transition: border-color 0.2s ease; box-sizing: border-box;" 
                       placeholder="https://example.com/document.pdf"
                       value="https://www2.census.gov/library/publications/2024/demo/p60-284.pdf">
                
                <label for="tableIdx" style="display: block; margin-bottom: 8px; font-weight: 600; color: #333333; font-size: 14px;">
                    Table Index <span style="font-weight: 400; color: #666; font-size: 13px;">(which table in the PDF to extract)</span>
                </label>
                <input type="number" id="tableIdx" style="width: 120px; padding: 14px; margin-bottom: 25px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 14px; transition: border-color 0.2s ease;" 
                       value="0" min="0">
                
                <button id="extractBtn" style="width: 100%; padding: 16px 24px; background: linear-gradient(135deg, #E5FF44 0%, #f0ff66 100%); color: #333333; border: none; border-radius: 8px; cursor: pointer; font-size: 16px; font-weight: 600; transition: transform 0.2s ease, box-shadow 0.2s ease; box-shadow: 0 2px 10px rgba(229, 255, 68, 0.3);">
                    📄 Download CSV
                </button>
            </div>

            <details style="margin-bottom: 15px; background: white; border-radius: 12px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); overflow: hidden;">
                <summary style="cursor: pointer; padding: 20px; background: #f8f9fa; font-weight: 600; color: #333333; font-size: 15px; border-bottom: 1px solid #e9ecef; transition: background 0.2s ease;">
                    ⚡ How it was built
                </summary>
                <div style="padding: 20px; color: #555;">
                    <div style="display: flex; align-items: flex-start; margin-bottom: 15px;">
                        <div style="background: #E5FF44; width: 6px; height: 6px; border-radius: 50%; margin: 8px 12px 0 0; flex-shrink: 0;"></div>
                        <p style="margin: 0; line-height: 1.6;">Every PDF gets sent to a Fused User Defined Function (UDF) that uses serverless Python to extract tables and return them as CSV</p>
                    </div>
                    <div style="display: flex; align-items: flex-start; margin-bottom: 15px;">
                        <div style="background: #E5FF44; width: 6px; height: 6px; border-radius: 50%; margin: 8px 12px 0 0; flex-shrink: 0;"></div>
                        <p style="margin: 0; line-height: 1.6;">This whole UI was <a href="https://docs.fused.io/tutorials/Analytics%20&%20Dashboard/#interactive-graphs-for-your-data" target="_blank" style="color: #333333; font-weight: 600; text-decoration: none; border-bottom: 2px solid #E5FF44;">vibe‑coded in another Fused UDF</a>, wrapping HTML around Python <a href="https://www.fused.io/workbench/udf/catalog/datalab_pdf_to_json_to_df-50eb6624-16b9-4347-9170-3a462b264357" target="_blank" style="color: #333333; font-weight: 600; text-decoration: none; border-bottom: 2px solid #E5FF44;">See the code for yourself</a></p>
                    </div>
                    <div style="display: flex; align-items: flex-start;">
                        <div style="background: #E5FF44; width: 6px; height: 6px; border-radius: 50%; margin: 8px 12px 0 0; flex-shrink: 0;"></div>
                        <p style="margin: 0; line-height: 1.6;"><a href="https://docs.fused.io/tutorials/Data%20Science%20&%20AI/pdf-scraping/" target="_blank" style="color: #333333; font-weight: 600; text-decoration: none; border-bottom: 2px solid #E5FF44;">Read how we built this</a></p>
                    </div>
                </div>
            </details>

            <div style="background: linear-gradient(135deg, #333333 0%, #4a4a4a 100%); padding: 25px; border-radius: 12px; text-align: center; box-shadow: 0 4px 20px rgba(51,51,51,0.2);">
                <p style="margin: 0; color: white; font-size: 15px; line-height: 1.5;">
                    Built by <strong style="color: #E5FF44;">Fused</strong> - the platform for serverless Python computing<br>
                    <a href="https://fused.io/workbench" target="_blank" style="display: inline-block; margin-top: 12px; padding: 10px 20px; background: #E5FF44; color: #333333; text-decoration: none; font-weight: 600; border-radius: 6px; transition: transform 0.2s ease;">🚀 Try Fused for yourself</a>
                </p>
            </div>
        </div>

        <style>
            #pdfUrl:focus, #tableIdx:focus {{
                outline: none;
                border-color: #E5FF44 !important;
                box-shadow: 0 0 0 3px rgba(229, 255, 68, 0.2);
            }}
            #extractBtn:hover {{
                transform: translateY(-2px);
                box-shadow: 0 4px 15px rgba(229, 255, 68, 0.4);
            }}
            #extractBtn:active {{
                transform: translateY(0);
            }}
            details[open] summary {{
                background: #E5FF44 !important;
                color: #333333 !important;
            }}
            summary:hover {{
                background: #f0f0f0 !important;
            }}
            a:hover {{
                text-decoration: underline !important;
            }}
        </style>

        <script>
        document.getElementById("extractBtn").onclick = function() {{
            const pdfUrl = document.getElementById("pdfUrl").value.trim();
            const tableIdx = document.getElementById("tableIdx").value;
            
            if (!pdfUrl) {{
                alert("Please enter a PDF URL first.");
                return;
            }}

            // Use the original working API URL
            const apiUrl = "https://unstable.fused.io/server/v1/realtime-shared/fsh_2cYag1UM0aM0c4HALo8Z3A/run/file";
            const params = new URLSearchParams({{
                "dtype_out_raster": "png",
                "dtype_out_vector": "csv",
                "pdf_url": pdfUrl,
                "raw_table_idx": tableIdx
            }});
            
            const fullUrl = `${{apiUrl}}?${{params.toString()}}`;
            
            // Log for debugging (console only)
            console.log("Generated URL:", fullUrl);
            console.log("PDF URL:", pdfUrl);
            console.log("Table Index:", tableIdx);
            
            // Open in new tab – this will trigger CSV download
            window.open(fullUrl, '_blank');
        }};

        // Allow Enter key to trigger extraction
        document.getElementById("pdfUrl").addEventListener("keypress", function(event) {{
            if (event.key === "Enter") {{
                document.getElementById("extractBtn").click();
            }}
        }});

        document.getElementById("tableIdx").addEventListener("keypress", function(event) {{
            if (event.key === "Enter") {{
                document.getElementById("extractBtn").click();
            }}
        }});
        </script>
    """

    return common.html_to_obj(html_content)