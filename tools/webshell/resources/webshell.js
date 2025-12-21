/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

/**
 * Global variables
 */

const OPTIONS = [
    'consistency',
    'expand',
    'output format',
    'paging',
    'serial consistency',
    'tracing',
]

const COMMANDS = [
    'help',
    'show session',
]

const Endpoint = {
  Command: '/command',
  Option: '/option',
  Query: '/query',
};

// Updated during and after login
let PROMPT = '$ ';

const commandHistory = [];
let commandHistoryIndex = null;

let currentQuery = null;
let pagingState = null;

const terminalWrapper = document.getElementById('terminal-wrapper')
const terminalInput = document.getElementById('terminal-input');
const terminalReadonly = document.getElementById('terminal-readonly')

/**
 * Function definitions
 */

function stripPrompt(s) {
    return s.slice(PROMPT.length);
}

function getCookiesAsObject() {
    const cookies = document.cookie.split('; ');
    const cookieObject = {};
    for (const cookie of cookies) {
        const firstEqualSignIndex = cookie.indexOf('=');
        const key = cookie.slice(0, firstEqualSignIndex);
        const value = cookie.slice(firstEqualSignIndex + 1);
        cookieObject[key] = value;
    }
    return cookieObject;
}

function cycleHistory(direction) {
    console.debug(`cycleHistory(${direction}): currentHistoryIndex=${commandHistoryIndex}`);

    if (commandHistory.length === 0) {
        return;
    }

    // Current command is not in history, so add it
    if (commandHistoryIndex === null) {
        const command = stripPrompt(terminalInput.value);
        if (command != commandHistory[commandHistory.length - 1]) {
            commandHistory.push(command);
        }
        commandHistoryIndex = commandHistory.length - 1;
    }

    commandHistoryIndex += direction;

    if (commandHistoryIndex < 0) {
        commandHistoryIndex = 0;
    } else if (commandHistoryIndex >= commandHistory.length) {
        commandHistoryIndex = commandHistory.length - 1;
    }

    terminalInput.value = PROMPT + commandHistory[commandHistoryIndex];
    terminalInput.setSelectionRange(terminalInput.value.length, terminalInput.value.length);
}

function append(text) {
    terminalReadonly.innerHTML += '<pre>' + text + '</pre>';
    window.scrollTo(0, terminalWrapper.scrollHeight);
}

// Check the keys of object to see if any is a prefix of command
function get_prefix(command, object) {
    for (const key of object) {
        if (command.trim().toLowerCase().startsWith(key.toLowerCase())) {
            return key;
        }
    }
    return null;
}

function prepare(command) {
    for (const obj of [[OPTIONS, Endpoint.Option, 'option'], [COMMANDS, Endpoint.Command, 'command']]) {
        const prefixes = obj[0];
        const endpoint = obj[1];
        const key = obj[2];
        prefix = get_prefix(command, prefixes);
        if (prefix !== null) {
            ret = {'endpoint': endpoint, 'body': {'arguments': command.slice(prefix.length).trim().split(' ')}};
            ret['body'][key] = prefix;
            return ret;
        }
    }
    return {'endpoint': Endpoint.Query, 'body': {'query': command}}
}

async function processCommand() {
    if (pagingState) {
        command = currentQuery;
    } else {
        value = terminalInput.value.trim()
        command = stripPrompt(value)

        console.debug(`processCommand(): command=${command}`);

        commandHistory.push(command);
        commandHistoryIndex = null;

        append(value);
        terminalInput.value = '';
        terminalInput.readOnly = true;

        if (command.trim() === 'exit' || command.trim() === 'quit') {
            const response = await fetch('/logout', { method: 'POST'})
            append(await response.json().response);
            PROMPT = '$ ';
            return newSession();
        }
    }

    const prepared_command = prepare(command);

    const isQuery = prepared_command.endpoint === Endpoint.Query;
    if (isQuery) {
        currentQuery = command;
        if (pagingState) {
            prepared_command.body.paging_state = pagingState;
        }
    } else {
        currentQuery = null;
    }

    const response = await fetch(prepared_command.endpoint, { method: 'POST', body: JSON.stringify(prepared_command.body)})

    responseJson = await response.json()
    append(responseJson.response);
    if (isQuery) {
        pagingState = responseJson.paging_state
        if (pagingState) {
            append(`-- More results available, press 'c' to show more or 'q' to quit --`);
        } else if (responseJson.trace_session_id) {
            append(`-- Obtain tracing information with 'SHOW SESSION ${responseJson.trace_session_id}'`);
        } else {
            terminalInput.readOnly = false;
            terminalInput.value = PROMPT;
        }
    } else {
        terminalInput.readOnly = false;
        terminalInput.value = PROMPT;
    }
}

function handlePrompt(event) {
    if (event.code === 'Backspace') {
        // Don't allow backspace to delete the prompt
        if (terminalInput.value.length == PROMPT.length) {
            event.preventDefault();
        }
    } else if (event.code === 'ArrowLeft') {
        if (terminalInput.selectionStart <= PROMPT.length) {
            // Don't allow left arrow to move the cursor before the prompt
            event.preventDefault();
        }
    }
}

function handleHistory(event) {
    let resetHistoryIndex = true;

    if (event.code === 'ArrowUp') {
        resetHistoryIndex = false;
        cycleHistory(-1);
        event.preventDefault();
    } else if (event.code === 'ArrowDown') {
        resetHistoryIndex = false;
        cycleHistory(+1);
        event.preventDefault();
    }

    if (resetHistoryIndex) {
        if (commandHistoryIndex !== null) {
            // Reset command history index if the user types something new
            commandHistoryIndex = null;
        }
        commandHistoryIndex = null;
    }
}

// This function filters events which correspond to characters allowed in passwords.
// It returns true if the character is allowed, false otherwise.
function filterPasswordChars(event) {
    // Allow only printable characters, excluding control characters and whitespace
    const charCode = event.which || event.keyCode;
    const c = String.fromCharCode(charCode);

    // Check if the character is a printable character
    if (c.match(/^[\x20-\x7E]$/)) {
        // Fix the case of the character, return upper-case if shift is pressed, lower-case otherwise
        // This is a simple way to handle case sensitivity in passwords
        // Note: This does not handle special characters like accents or non-ASCII characters
        // It is a basic implementation and may need to be adjusted for specific requirements
        return event.shiftKey ? c.toUpperCase() : c.toLowerCase();
    }

    return null;
}

function onLogin(response) {
    append(response.response);

    const cookies = getCookiesAsObject()
    PROMPT = `${cookies['user_name']}@${cookies['cluster_name']} $ `;

    terminalInput.readOnly = false;
    terminalInput.value = PROMPT;

    terminalInput.onkeydown = (event => {
        if (event.code === 'Enter') {
            event.preventDefault();
            return processCommand();
        }
        if (pagingState) {
            event.preventDefault();
            if (event.key === 'q') {
                pagingState = null;
                terminalInput.readOnly = false;
                terminalInput.value = PROMPT;
            } else if (event.key === 'c') {
                return processCommand();
            }
        }

        handlePrompt(event);
        handleHistory(event);
    });
}

async function newSession(anonymous = true) {
    if (anonymous) {
        login_response = await fetch('/login', { method: 'POST' })
        if (!login_response.ok) {
            console.log('Anonymous login failed, requesting user credentials');
            return newSession(false);
        }
        onLogin(await login_response.json());
        return;
    }

    let user = null;
    let password = '';

    PROMPT = 'login: ';
    terminalInput.value = PROMPT;
    terminalInput.readOnly = false;

    terminalInput.onkeydown = (event => {
        if (event.code === 'Enter') {
            if (user === null) {
                user = stripPrompt(terminalInput.value);
                PROMPT = 'password: ';
                terminalInput.value = PROMPT;
            } else {
                terminalInput.type = 'text';

                fetch('/login', { method: 'POST', body: `${user}\n${password}` }).then(response => {
                    if (response.ok) {
                        return response.json().then(onLogin);
                    } else {
                        return response.json().then(responseJson => {
                            if (response.status === 400) {
                                append(`Login failed: ${responseJson.response}`);
                                return newSession(false);
                            } else {
                                append(`Login failed with unexpected error (${response.statusText}): ${responseJson.response}`);
                                terminalInput.readOnly = true;
                                terminalInput.value = '';
                            }
                        });
                    }
                });
            }

            event.preventDefault();
            return;
        }

        if (event.code === 'ArrowUp' || event.code === 'ArrowDown') {
            event.preventDefault();
        }

        handlePrompt(event);

        if (user != null) {
            if (event.code === 'Backspace') {
                password = password.slice(0, -1);
            } else {
                const c = filterPasswordChars(event);
                if (c !== null) {
                    password += c;
                }
            }
            event.preventDefault();
        }
    });
}

/*
 * Main function
 */

terminalInput.focus();

terminalWrapper.onclick = (event => {
    // Focus the input field if the user clicks anywhere in the terminal
    terminalInput.focus();
})

append('ScyllaDB WebShell');
append('!!! WebShell is still experimental, things are subject to change and there may be bugs !!!');
append('For help type HELP, or visit the <a href="https://docs.scylladb.com/manual/master/operating-scylla/admin-tools/webshell.html">documentation</a>.');

newSession()
