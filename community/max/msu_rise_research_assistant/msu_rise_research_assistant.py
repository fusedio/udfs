@fused.udf
def udf():
    """
    Returns a fully-functional single-page HTML chat assistant that mimics the
    original Streamlit app.  The HTML embeds the FAQ data, logo and avatar
    images (base64) and uses pure JavaScript for interactivity (category
    filtering, fuzzy matching, suggestions, download of chat history, etc.).
    Added: PDF upload button + drag‑and‑drop area.
    """
    import pandas as pd
    import requests
    import base64
    from io import BytesIO
    from difflib import SequenceMatcher, get_close_matches

    # ------------------------------------------------------------------
    # Helper: fetch remote files and cache them
    # ------------------------------------------------------------------
    @fused.cache
    def fetch_text(url: str) -> str:
        """Simple GET request returning raw text."""
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.text

    @fused.cache
    def fetch_image_base64(url: str) -> str:
        """Download an image and return a base64-encoded PNG string."""
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return base64.b64encode(resp.content).decode()
        except Exception:
            return ""

    # ------------------------------------------------------------------
    # Load assets (logo, avatar, FAQ CSV)
    # ------------------------------------------------------------------
    LOGO_URL = "https://raw.githubusercontent.com/ManaKashuk/MSU-RISE/main/logo.png"
    AVATAR_URL = "https://raw.githubusercontent.com/ManaKashuk/MSU-RISE/main/chat.png"
    CSV_URL = "https://raw.githubusercontent.com/ManaKashuk/MSU-RISE/main/msu_faq.csv"

    logo_base64 = fetch_image_base64(LOGO_URL)
    avatar_base64 = fetch_image_base64(AVATAR_URL)

    # Load the FAQ CSV – if it fails, create an empty placeholder
    try:
        csv_text = fetch_text(CSV_URL)
        from io import StringIO
        df = pd.read_csv(StringIO(csv_text)).fillna("")
    except Exception:
        df = pd.DataFrame(columns=["Category", "Question", "Answer"])

    # ------------------------------------------------------------------
    # Prepare data structures for the front-end
    # ------------------------------------------------------------------
    faq_data = df.to_dict(orient="records")
    categories = ["All Categories"] + sorted(df["Category"].unique().tolist()) if not df.empty else ["All Categories"]

    # ------------------------------------------------------------------
    # Build the final HTML (escaped braces for JS objects)
    # ------------------------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>MSU Research Security Assistant</title>
<style>
    body {{ font-family: Arial, sans-serif; background:#fafafa; margin:0; padding:0; }}
    .container {{ max-width: 900px; margin: auto; padding:20px; }}
    .header {{ text-align:left; }}
    .header img {{ max-width:100%; height:auto; }}
    .header h2 {{ margin:10px 0 5px; }}
    .header h5 {{ margin:0 0 15px; color:#555; font-weight:normal; }}
    .chat-box {{ border:1px solid #ddd; border-radius:8px; background:#fff; padding:15px; max-height:500px; overflow-y:auto; }}
    .msg-user {{ text-align:right; margin:10px 0; }}
    .msg-user .bubble {{ display:inline-block; background:#e6f7ff; padding:10px 15px; border-radius:12px; max-width:70%; }}
    .msg-assist {{ text-align:left; margin:10px 0; }}
    .msg-assist .bubble {{ display:inline-block; background:#f6f6f6; padding:10px 15px; border-radius:12px; max-width:75%; }}
    .msg-assist .bubble img {{ vertical-align:middle; border-radius:8px; margin-right:8px; }}
    .input-area {{ margin-top:15px; display:flex; gap:10px; flex-wrap:wrap; }}
    .input-area input {{ flex:1; padding:8px; font-size:1rem; }}
    .input-area button {{ padding:8px 16px; font-size:1rem; cursor:pointer; }}
    .file-drop {{ border:2px dashed #bbb; border-radius:8px; padding:20px; text-align:center; color:#777; margin-top:10px; }}
    .file-drop.dragover {{ border-color:#666; background:#f0f0f0; }}
    .suggestions {{ margin-top:5px; }}
    .suggestion-btn {{ display:inline-block; margin:3px 5px 0 0; background:#eee; border:none; padding:5px 10px; border-radius:5px; cursor:pointer; }}
    .download-link {{ margin-top:15px; display:block; }}
    .footer {{ margin-top:30px; font-size:0.9rem; color:#777; text-align:center; }}
    .disclaimer {{ font-size:0.8rem; color:#555; margin-top:10px; }}
</style>
</head>
<body>
<div class="container">

<div class="header">
    {"<img src='data:image/png;base64," + logo_base64 + "' width='700'/>" if logo_base64 else ""}
    <h2>🛡️MSU Research Security Assistant🛡️</h2>
    <h5><i>🧠 Smart Assistant for Research Integrity, Compliance &amp; Security Support</i></h5>
    <p>Trained on MSU Office of Research Compliance Internal SOPs and Federal Guidance.</p>
</div>

<div style="margin:20px 0;">
    <label for="categorySelect"><strong>📂 Select a category:</strong></label>
    <select id="categorySelect">
        {"".join([f"<option value='{c}'>{c}</option>" for c in categories])}
    </select>
</div>

<div class="chat-box" id="chatBox"></div>

<div class="input-area">
    <input type="text" id="questionInput" placeholder="💬 Start typing your question..." />
    <button id="submitBtn">Submit</button>
    <button id="uploadBtn">Upload PDF</button>
    <input type="file" id="pdfUpload" accept="application/pdf" style="display:none;">
</div>

<div class="file-drop" id="dropZone">Drag &amp; drop PDF here</div>

<div class="suggestions" id="suggestionBox"></div>
<a id="downloadLink" class="download-link" href="#" style="display:none;">📥 Download Chat History</a>

<div class="disclaimer">
    ⚖️ Disclaimer: This is a demo tool. For official guidance, refer to MSU policies. 💻 It offers AI-powered answers based on MSU-specific rules, NSPM-33, and quick links to forms and training. 📚 Designed to support under-resourced teams by streamlining award workflows and boosting compliance, without accessing sensitive data. ⚖️
</div>

<div class="footer">
    <hr style="margin:0.5rem 0;">
    © 2025 Morgan State University Office of Research Administration
</div>

</div>

<script>
    // ------------------------------------------------------------------
    // Data from the backend
    // ------------------------------------------------------------------
    const faqData = {faq_data};

    // ------------------------------------------------------------------
    // Utility functions
    // ------------------------------------------------------------------
    function escapeHTML(str) {{
        const div = document.createElement('div');
        div.appendChild(document.createTextNode(str));
        return div.innerHTML;
    }}

    function similarity(a, b) {{
        function lcs(x, y) {{
            const dp = Array(x.length+1).fill().map(()=>Array(y.length+1).fill(0));
            for(let i=1;i<=x.length;i++) {{
                for(let j=1;j<=y.length;j++) {{
                    if(x[i-1]===y[j-1]) dp[i][j]=dp[i-1][j-1]+1;
                    else dp[i][j]=Math.max(dp[i-1][j],dp[i][j-1]);
                }}
            }}
            return dp[x.length][y.length];
        }}
        const longest = Math.max(a.length, b.length);
        if (longest===0) return 1;
        return lcs(a.toLowerCase(), b.toLowerCase())/longest;
    }}

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------
    let chatHistory = [];
    let currentCategory = "All Categories";

    // ------------------------------------------------------------------
    // DOM Elements
    // ------------------------------------------------------------------
    const chatBox = document.getElementById('chatBox');
    const questionInput = document.getElementById('questionInput');
    const submitBtn = document.getElementById('submitBtn');
    const categorySelect = document.getElementById('categorySelect');
    const suggestionBox = document.getElementById('suggestionBox');
    const downloadLink = document.getElementById('downloadLink');
    const uploadBtn = document.getElementById('uploadBtn');
    const pdfUpload = document.getElementById('pdfUpload');
    const dropZone = document.getElementById('dropZone');

    // ------------------------------------------------------------------
    // Render functions
    // ------------------------------------------------------------------
    function renderChat() {{
        chatBox.innerHTML = '';
        chatHistory.forEach(msg => {{
            const div = document.createElement('div');
            div.className = msg.role === 'user' ? 'msg-user' : 'msg-assist';
            const bubble = document.createElement('div');
            bubble.className = 'bubble';
            if (msg.role === 'assistant') {{
                const avatarImg = document.createElement('img');
                avatarImg.src = "data:image/png;base64,{avatar_base64}";
                avatarImg.width = 40;
                bubble.appendChild(avatarImg);
                const span = document.createElement('span');
                span.innerHTML = msg.content;
                bubble.appendChild(span);
            }} else {{
                bubble.innerHTML = `<b>You:</b> ` + msg.content;
            }}
            div.appendChild(bubble);
            chatBox.appendChild(div);
        }});
        chatBox.scrollTop = chatBox.scrollHeight;
        updateDownloadLink();
    }}

    function updateDownloadLink() {{
        if (chatHistory.length===0) {{
            downloadLink.style.display = 'none';
            return;
        }}
        const txt = chatHistory.map(m=> (m.role==='user'?'You':'Assistant') + ': ' + m.content).join('\\n\\n');
        const b64 = btoa(unescape(encodeURIComponent(txt)));
        downloadLink.href = `data:text/plain;base64,${{b64}}`;
        downloadLink.download = 'chat_history.txt';
        downloadLink.style.display = 'block';
    }}

    function showSuggestions(suggestions) {{
        suggestionBox.innerHTML = '';
        if (suggestions.length===0) return;
        const label = document.createElement('div');
        label.innerHTML = '<b>Suggestions:</b>';
        suggestionBox.appendChild(label);
        suggestions.forEach(s => {{
            const btn = document.createElement('button');
            btn.className = 'suggestion-btn';
            btn.textContent = s;
            btn.onclick = () => {{
                questionInput.value = s;
                suggestionBox.innerHTML = '';
            }};
            suggestionBox.appendChild(btn);
        }});
    }}

    // ------------------------------------------------------------------
    // Core logic
    // ------------------------------------------------------------------
    function filterFAQByCategory(cat) {{
        if (cat === 'All Categories') return faqData;
        return faqData.filter(row => row.Category === cat);
    }}

    function getTopExamples() {{
        const filtered = filterFAQByCategory(currentCategory);
        return filtered.slice(0,3).map(r=> r.Question);
    }}

    function addUserMessage(text) {{
        chatHistory.push({{role:'user', content: escapeHTML(text)}});
        renderChat();
    }}

    function addAssistantMessage(html) {{
        chatHistory.push({{role:'assistant', content: html}});
        renderChat();
    }}

    function processQuestion(rawQ) {{
        const filteredFAQ = filterFAQByCategory(currentCategory);
        const allQuestions = filteredFAQ.map(r=> r.Question);
        const exact = filteredFAQ.find(r=> r.Question.toLowerCase()===rawQ.toLowerCase());
        if (exact) {{
            const resp = `<b>Answer:</b> ${{escapeHTML(exact.Answer)}}<br><i>(Note: This question belongs to the '${{escapeHTML(exact.Category)}}' category.)</i>`;
            addAssistantMessage(resp);
            return;
        }}

        // fuzzy match (ratio >= 0.85)
        let best = null, bestScore = 0;
        filteredFAQ.forEach(r=> {{
            const score = similarity(rawQ, r.Question);
            if (score > bestScore) {{
                bestScore = score;
                best = r;
            }}
        }});
        if (best && bestScore >= 0.85) {{
            const resp = `<b>Answer:</b> ${{escapeHTML(best.Answer)}}<br><i>(Note: This question belongs to the '${{escapeHTML(best.Category)}}' category.)</i>`;
            addAssistantMessage(resp);
            return;
        }}

        // suggestions based on substring
        const suggestions = allQuestions.filter(q=> q.toLowerCase().includes(rawQ.toLowerCase())).slice(0,5);
        if (suggestions.length>0) {{
            showSuggestions(suggestions);
            const note = `I couldn't find an exact match. Here are some similar questions:`;
            addAssistantMessage(note);
            return;
        }}

        // fallback to global close matches (using difflib style)
        const globalAll = faqData.map(r=> r.Question);
        const close = getCloseMatches(rawQ, globalAll, 3, 0.4);
        if (close.length>0) {{
            const guessedCat = faqData.find(r=> r.Question===close[0]).Category;
            let txt = `I couldn't find an exact match, but your question seems related to <b>${{escapeHTML(guessedCat)}}</b>.<br><br>Here are some similar questions:<br>`;
            close.forEach((q,i)=>{{ txt += `${{i+1}}. ${{escapeHTML(q)}}<br>`; }});
            txt += `<br>Select one above to see its answer.<br><br><a href="mailto:research.compliance@morgan.edu">research.compliance@morgan.edu</a>`;
            addAssistantMessage(txt);
            window.__globalSuggestions = close;
            return;
        }}

        // ultimate fallback
        addAssistantMessage(`I couldn't find a close match. Please try rephrasing.<br><br><a href="mailto:research.compliance@morgan.edu">research.compliance@morgan.edu</a>`);
    }}

    // ------------------------------------------------------------------
    // PDF handling
    // ------------------------------------------------------------------
    function handlePDFFile(file) {{
        if (!file) return;
        if (file.type !== 'application/pdf') {{
            alert('Please upload a PDF file.');
            return;
        }}
        // Simple feedback – you could extend to upload to S3 or parse contents
        addUserMessage(`Uploaded PDF: ${{escapeHTML(file.name)}}`);
        // Example: read as base64 (optional)
        const reader = new FileReader();
        reader.onload = function(e) {{
            const base64 = e.target.result.split(',')[1];
            console.log('PDF base64 (first 100 chars):', base64.slice(0,100));
            // You could store this in a variable for later use if needed
        }};
        reader.readAsDataURL(file);
    }}

    // Button click → hidden file input
    uploadBtn.onclick = () => pdfUpload.click();

    // File input change
    pdfUpload.onchange = (e) => {{
        const file = e.target.files[0];
        handlePDFFile(file);
        // Reset the input so same file can be selected again if desired
        e.target.value = "";
    }};

    // Drag & drop events
    dropZone.addEventListener('dragover', (e) => {{
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.add('dragover');
    }});
    dropZone.addEventListener('dragleave', (e) => {{
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.remove('dragover');
    }});
    dropZone.addEventListener('drop', (e) => {{
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.remove('dragover');
        const file = e.dataTransfer.files[0];
        handlePDFFile(file);
    }});

    // ------------------------------------------------------------------
    // Event listeners
    // ------------------------------------------------------------------
    submitBtn.onclick = () => {{
        const q = questionInput.value.trim();
        if (!q) return;
        addUserMessage(q);
        processQuestion(q);
        questionInput.value = '';
        suggestionBox.innerHTML = '';
    }};

    questionInput.oninput = () => {{
        const q = questionInput.value.trim();
        if (q) {{
            const filteredFAQ = filterFAQByCategory(currentCategory);
            const matches = filteredFAQ.map(r=> r.Question).filter(t=> t.toLowerCase().includes(q.toLowerCase())).slice(0,5);
            showSuggestions(matches);
        }} else {{
            suggestionBox.innerHTML = '';
        }}
    }};

    categorySelect.onchange = () => {{
        currentCategory = categorySelect.value;
        // reset chat when category changes
        chatHistory = [];
        renderChat();
        questionInput.value = '';
        suggestionBox.innerHTML = '';
    }};

    // ------------------------------------------------------------------
    // Initial render
    // ------------------------------------------------------------------
    renderChat();
</script>
</body>
</html>
"""
    # Debug: print a short part of the generated HTML schema
    print("HTML length:", len(html_content))
    return html_content