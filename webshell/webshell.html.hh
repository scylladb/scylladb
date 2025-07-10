/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

namespace webshell {

// Putting this in a separate file, so one can do :set filetype=html (or the equivalent in your inferior editor) for a nicer editing experience.
constexpr char webshell_html[] = R"(

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ScyllaDB Web Shell</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            background-color: #f4f4f4;
            color: #333;
        }
        #shell {
            width: 100%;
            height: 300px;
            background-color: #fff;
            border: 1px solid #ccc;
            padding: 10px;
            overflow-y: auto;
        }
        input[type="text"] {
            width: calc(100% - 22px);
            padding: 10px;
            margin-top: 10px;
        }
    </style>
</head>
<body>
    <h1>ScyllaDB Web Shell</h1>
    <div id="shell"></div>
    <input type="text" id="operation" placeholder="Enter operation here..." autofocus>

    <script>
        const shell = document.getElementById('shell');
        const operation_input = document.getElementById('operation');
        const xhttp = new XMLHttpRequest();

        xhttp.onload = function() {
            if (this.status >= 200 && this.status < 300) {
                shell.innerHTML += `<div><pre>${this.responseText}</pre></div>`;
                shell.scrollTop = shell.scrollHeight; // Scroll to the bottom
            } else {
                shell.innerHTML += `<div style="color: red;">Error: ${this.statusText}</div>`;
            }
        };

        function sendRequest(path, request) {
            xhttp.open('POST', '/' + path, true);
            xhttp.setRequestHeader('Content-Type', 'text/plain');
            xhttp.send(request);
        }

        sendRequest('login', '');

        operation_input.addEventListener('keydown', function(event) {
            if (event.key === 'Enter') {
                const operation = operation_input.value;
                if (operation.trim() !== '') {
                    executeOperation(operation);
                    operation_input.value = '';
                }
            }
        });

        function isRecognizedOption(operation) {
            operation = operation.trim().toLowerCase();
            if (operation.startsWith('output-format')) {
                return true;
            }
            return false;
        }

        function executeOperation(operation) {
            shell.innerHTML += `<div><strong><pre>$ ${operation}</pre></strong></div>`;
            if (isRecognizedOption(operation)) {
                sendRequest('option', operation);
            } else {
                sendRequest('query', operation);
            }
        }
    </script>
    <script>
        function clearShell() {
            shell.innerHTML = '';
        }
    </script>
</body>
</html>

)";

} // namespace webshell
