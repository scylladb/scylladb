/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

'use strict';

/**
 * Constants
 */

const QUERY_ENDPOINT = '/query';

const Phase = {
    LoginUser: 'login-user',
    LoginPassword: 'login-password',
    Ready: 'ready',
    Busy: 'busy',
    More: 'more',
};

// The options, and the values each one accepts. The options are kept here, and
// each query request carries the ones the server needs. The rest (expand,
// output format) only affect how results are shown.
const OPTIONS = {
    'consistency': ['ANY', 'ONE', 'TWO', 'THREE', 'QUORUM', 'ALL', 'LOCAL_QUORUM', 'EACH_QUORUM',
                    'SERIAL', 'LOCAL_SERIAL', 'LOCAL_ONE'],
    'expand': ['ON', 'OFF'],
    'output format': ['TEXT', 'JSON'],
    'paging': ['ON'],
    'serial consistency': ['SERIAL', 'LOCAL_SERIAL'],
    'tracing': ['ON', 'OFF'],
};

// Commands, and the values each one accepts.
const COMMANDS = {
    'help': [],
    'show session': [],
};

// The page size limits of the server, see tools/webshell/webshell.cc.
const DEFAULT_PAGE_SIZE = 100;
const MAX_PAGE_SIZE = 1000;

// Run to find out whether the cluster requires authentication, and to check
// credentials. Readable by every role.
const PROBE_QUERY = 'SELECT cluster_name FROM system.local';

// Handled here in the client rather than by an endpoint.
const BUILTINS = ['clear', 'exit', 'quit'];

const CQL_KEYWORDS = [
    'ALL', 'ALLOW', 'ALTER', 'AND', 'APPLY', 'AS', 'ASC', 'BATCH', 'BEGIN', 'BY', 'CLUSTERING',
    'COLUMNFAMILY', 'CONTAINS', 'COUNT', 'CREATE', 'DELETE', 'DESC', 'DESCRIBE', 'DISTINCT',
    'DROP', 'EXISTS', 'FILTERING', 'FROM', 'GRANT', 'IF', 'IN', 'INDEX', 'INSERT', 'INTO', 'IS',
    'JSON', 'KEY', 'KEYSPACE', 'LIMIT', 'MATERIALIZED', 'NOT', 'NULL', 'ON', 'ORDER', 'PARTITION',
    'PER', 'PRIMARY', 'REVOKE', 'ROLE', 'SELECT', 'SET', 'STATIC', 'STORAGE', 'TABLE', 'TIMESTAMP',
    'TO', 'TOKEN', 'TRUNCATE', 'TTL', 'TYPE', 'UNLOGGED', 'UPDATE', 'USE', 'USER', 'USING',
    'VALUES', 'VIEW', 'WHERE', 'WITH', 'WRITETIME',
];

// Longest name first, so "serial consistency" wins over "consistency" and
// "show session" over "show".
const DIRECTIVES = [
    ...Object.keys(OPTIONS).map((name) => ({ name, key: 'option' })),
    ...Object.keys(COMMANDS).map((name) => ({ name, key: 'command' })),
].sort((a, b) => b.name.length - a.name.length);

const DIRECTIVE_NAMES = DIRECTIVES.map((d) => d.name).sort();

// Offered when completing at the start of a statement.
const STATEMENT_STARTS = [...DIRECTIVE_NAMES, ...BUILTINS, ...CQL_KEYWORDS].sort();

// The defaults of the options, the same as the server's defaults for the ones
// sent with each query.
const DEFAULT_OPTIONS = {
    consistency: 'ONE',
    expand: false,
    output_format: 'TEXT',
    page_size: DEFAULT_PAGE_SIZE,
    serial_consistency: 'SERIAL',
    tracing: false,
};

// Which member of the options each option statement sets.
const OPTION_MEMBER = {
    'consistency': 'consistency',
    'expand': 'expand',
    'output format': 'output_format',
    'paging': 'page_size',
    'serial consistency': 'serial_consistency',
    'tracing': 'tracing',
};

// Each option is reported as the statement that would put it where it now
// stands, which reads the same whether the option was just changed or only
// asked about - and can be typed back verbatim to restore the value later.
const OPTION_ECHO = {
    consistency: (v) => `CONSISTENCY ${v}`,
    expand: (v) => `EXPAND ${v ? 'ON' : 'OFF'}`,
    output_format: (v) => `OUTPUT FORMAT ${v}`,
    page_size: (v) => `PAGING ${v}`,
    serial_consistency: (v) => `SERIAL CONSISTENCY ${v}`,
    tracing: (v) => `TRACING ${v ? 'ON' : 'OFF'}`,
};

const HELP_TEXT = `Type CQL statements, terminated by ";", to execute them.

Commands:
 * HELP - show this message.
 * SHOW SESSION <tracing-session-id> - show the events of a tracing session.
 * CLEAR - clear the screen (also Ctrl+L).
 * EXIT, QUIT - sign out.

Options (with no argument, show the current value):
 * CONSISTENCY [<level>] - the consistency level of queries (default: ONE).
 * SERIAL CONSISTENCY [SERIAL|LOCAL_SERIAL] - the serial consistency level of
   conditional updates (default: SERIAL).
 * PAGING [ON|<page size>] - the page size of queries, at most ${MAX_PAGE_SIZE}
   (default: ${DEFAULT_PAGE_SIZE}). Paging cannot be turned off.
 * TRACING [ON|OFF] - trace queries (default: OFF).
 * EXPAND [ON|OFF] - show each row as a separate table (default: OFF).
 * OUTPUT FORMAT [TEXT|JSON] - show results as tables or as the JSON response
   of the server (default: TEXT).`;

const DOCS_URL = 'https://docs.scylladb.com/manual/master/operating-scylla/admin-tools/webshell.html';

/**
 * State
 */

const state = {
    phase: Phase.LoginUser,
    user: null,
    cluster: null,
    // The value of the Authorization header sent with each request, null for
    // anonymous access. Kept in memory only: reloading the page signs out.
    credentials: null,
    // The current keyspace, set by USE <keyspace>. Sent with each query.
    keyspace: null,
    options: { ...DEFAULT_OPTIONS },
    history: [],
    historyIndex: null,
    historyDraft: '',
    pagingState: null,
    pagingQuery: null,
    // The number of rows shown so far of the query being paged, so the rows of
    // later pages are numbered on from there in expanded mode.
    pagingRows: 0,
    busyBlock: null,
    moreBlock: null,
    loginUser: null,
};

/**
 * Elements
 */

const screenEl = document.getElementById('screen');
const scrollbackEl = document.getElementById('scrollback');
const rowEl = document.getElementById('row');
const rowMarkEl = document.getElementById('row-mark');
const inputEl = document.getElementById('input');
const passwordEl = document.getElementById('password');
const identityEl = document.getElementById('identity');
const signoutEl = document.getElementById('signout');
const statusEl = document.getElementById('status');

/**
 * Output
 */

function scrollToBottom() {
    screenEl.scrollTop = screenEl.scrollHeight;
}

// Builds one line-block of the scrollback. Text always goes in via textContent:
// query results are arbitrary user data and must never be parsed as markup.
function printBlock(kind, text, { mark = '', prompt = null } = {}) {
    const block = document.createElement('div');
    block.className = `block block--${kind}`;

    const markEl = document.createElement('span');
    markEl.className = 'mark';
    markEl.textContent = mark;
    block.appendChild(markEl);

    if (prompt) {
        block.appendChild(prompt);
    }

    const body = document.createElement('div');
    body.className = 'body';
    if (text instanceof Node) {
        body.appendChild(text);
    } else {
        body.textContent = text;
    }
    block.appendChild(body);

    scrollbackEl.appendChild(block);
    scrollToBottom();
    return block;
}

function printOutput(text) {
    if (text !== '') {
        printBlock('output', text);
    }
}

function printError(text) {
    printBlock('error', text, { mark: '!' });
}

function printNotice(text) {
    printBlock('notice', text, { mark: '·' });
}

function printBlank() {
    printBlock('output', ' ');
}

function printBanner() {
    const frag = document.createDocumentFragment();

    const title = document.createElement('b');
    title.textContent = 'ScyllaDB Web Shell';
    frag.append(title, '\n');
    frag.append('Experimental: behaviour and output may change between releases.\n');
    frag.append('Type HELP for commands and options, or read the ');

    const link = document.createElement('a');
    link.href = DOCS_URL;
    link.textContent = 'documentation';
    frag.append(link, '.\n\n');
    // Shift+Tab is mentioned because Tab is taken over for completion, and a
    // keyboard user has no other way to guess how to get out of the input.
    frag.append('Enter runs a statement once it ends in ";". Shift+Enter adds a line, '
                + 'Tab completes, Shift+Tab leaves the input.');

    printBlock('banner', frag);
    printBlank();
}

/**
 * Prompt
 */

// The prompt is its own element rather than text inside the input, so the
// input behaves like an ordinary text field: Home, Ctrl+A, paste and clicking
// all work without being second-guessed by a keydown handler.
function buildPrompt() {
    const el = document.createElement('span');
    el.className = 'prompt';

    if (state.phase === Phase.LoginUser) {
        el.textContent = 'login: ';
    } else if (state.phase === Phase.LoginPassword) {
        el.textContent = 'password: ';
    } else if (state.user !== null) {
        const user = document.createElement('span');
        user.className = 'prompt-user';
        user.textContent = state.user;

        const host = document.createElement('span');
        host.className = 'prompt-host';
        host.textContent = state.cluster ? `@${state.cluster}` : '';

        const keyspace = document.createElement('span');
        keyspace.className = 'prompt-keyspace';
        keyspace.textContent = state.keyspace ? `:${state.keyspace}` : '';

        el.append(user, host, keyspace, '> ');
    }

    return el;
}

// The live prompt element is replaced wholesale rather than mutated, so the
// same buildPrompt() output is used for both the prompt you type at and the
// prompt that gets echoed into the scrollback.
function refreshPromptEl() {
    const fresh = buildPrompt();
    fresh.id = 'prompt';
    document.getElementById('prompt').replaceWith(fresh);
}

function echoInput(text) {
    printBlock('echo', text, { prompt: buildPrompt() });
}

/**
 * Status bar
 */

function statusSegment(label, value, changed) {
    const seg = document.createElement('span');
    seg.className = changed ? 'opt opt--changed' : 'opt';
    seg.append(`${label} `);

    const val = document.createElement('b');
    val.textContent = value;
    seg.appendChild(val);

    return seg;
}

function renderStatus() {
    statusEl.replaceChildren();

    if (state.user === null) {
        statusEl.appendChild(statusSegment('user', 'none', false));
        return;
    }

    const o = state.options;
    statusEl.append(
        statusSegment('cl', o.consistency, o.consistency !== DEFAULT_OPTIONS.consistency),
        statusSegment('serial', o.serial_consistency, o.serial_consistency !== DEFAULT_OPTIONS.serial_consistency),
        statusSegment('paging', String(o.page_size), o.page_size !== DEFAULT_OPTIONS.page_size),
        statusSegment('format', o.output_format.toLowerCase(), o.output_format !== DEFAULT_OPTIONS.output_format),
        statusSegment('expand', o.expand ? 'on' : 'off', o.expand),
        statusSegment('tracing', o.tracing ? 'on' : 'off', o.tracing),
    );
}

// Show or change an option. Returns the error message, if the arguments are
// not valid for the option, null otherwise.
function runOption(name, args) {
    const member = OPTION_MEMBER[name];

    if (args.length === 0) {
        printOutput(OPTION_ECHO[member](state.options[member]));
        return null;
    }
    if (args.length > 1) {
        return `Too many arguments for ${name.toUpperCase()}`;
    }

    const arg = args[0].toUpperCase();
    let value;
    switch (name) {
        case 'consistency':
        case 'serial consistency':
        case 'output format':
            if (!OPTIONS[name].includes(arg)) {
                return `Invalid ${name.toUpperCase()} value ${args[0]}, expected one of: ${OPTIONS[name].join(', ')}`;
            }
            value = arg;
            break;
        case 'expand':
        case 'tracing':
            if (arg !== 'ON' && arg !== 'OFF') {
                return `Invalid ${name.toUpperCase()} value ${args[0]}, expected ON or OFF`;
            }
            value = arg === 'ON';
            break;
        case 'paging':
            if (arg === 'ON') {
                value = DEFAULT_PAGE_SIZE;
            } else if (arg === 'OFF') {
                return `Paging cannot be turned off, the largest page size is ${MAX_PAGE_SIZE}`;
            } else if (/^[0-9]+$/.test(arg) && Number(arg) >= 1 && Number(arg) <= MAX_PAGE_SIZE) {
                value = Number(arg);
            } else {
                return `Invalid PAGING value ${args[0]}, expected ON or a page size between 1 and ${MAX_PAGE_SIZE}`;
            }
            break;
        default:
            return `Unknown option ${name}`;
    }

    state.options[member] = value;
    renderStatus();
    printOutput(OPTION_ECHO[member](value));
    return null;
}

/**
 * Transport
 */

// Encode the credentials for the Authorization header. btoa() only takes
// Latin-1, so the UTF-8 bytes of the credentials are passed through it.
function basicCredentials(username, password) {
    const bytes = new TextEncoder().encode(`${username}:${password}`);
    return `Basic ${btoa(String.fromCharCode(...bytes))}`;
}

// The members of a query request, besides the query itself: everything the
// server needs to know to execute it.
function queryOptions() {
    const o = state.options;
    const body = {
        consistency: o.consistency,
        serial_consistency: o.serial_consistency,
        page_size: o.page_size,
        tracing: o.tracing,
    };
    if (state.keyspace) {
        body.keyspace = state.keyspace;
    }
    return body;
}

// Send a query request. Every response is a JSON object, an error response
// carries its message in the "error" member.
async function postQuery(body, credentials = state.credentials) {
    const headers = { 'Content-Type': 'application/json' };
    if (credentials) {
        headers.Authorization = credentials;
    }

    let response;
    try {
        response = await fetch(QUERY_ENDPOINT, {
            method: 'POST',
            headers,
            body: JSON.stringify(body),
            cache: 'no-store',
        });
    } catch (e) {
        return { ok: false, status: 0, message: `Cannot reach the server: ${e.message}` };
    }

    let json = null;
    try {
        json = await response.json();
    } catch (e) {
        // Truncated or non-JSON body; fall back to the status line below.
    }

    return {
        ok: response.ok && json !== null,
        status: response.status,
        json,
        message: json?.error ?? (response.statusText || `HTTP ${response.status}`),
    };
}

/**
 * Results
 */

// Values are formatted the same as with SELECT JSON: strings are shown as they
// are, collections, tuples and user types as JSON.
function formatValue(value) {
    if (value === null || value === undefined) {
        return 'null';
    }
    return typeof value === 'string' ? value : JSON.stringify(value);
}

function rowValues(columns, row) {
    return columns.map((col) => formatValue(row[col.name]));
}

// cqlsh style: the column names left aligned, the values right aligned.
function formatTable(columns, rows) {
    const values = rows.map((row) => rowValues(columns, row));
    const widths = columns.map((col, i) => Math.max(col.name.length, ...values.map((v) => v[i].length)));

    const lines = [];
    lines.push(columns.map((col, i) => ` ${col.name.padEnd(widths[i])} `).join('|'));
    lines.push(widths.map((w) => '-'.repeat(w + 2)).join('+'));
    for (const v of values) {
        lines.push(v.map((value, i) => ` ${value.padStart(widths[i])} `).join('|'));
    }
    return lines.join('\n');
}

// Each row as a separate table, column name and value in each line, for rows
// too wide to read as a single line.
function formatExpanded(columns, rows, firstRowNumber) {
    const nameWidth = Math.max(...columns.map((col) => col.name.length));
    const blocks = rows.map((row, r) => {
        const values = rowValues(columns, row);
        const valueWidth = Math.max(1, ...values.map((v) => v.length));
        const lines = [`@ Row ${firstRowNumber + r}`, `${'-'.repeat(nameWidth + 2)}+${'-'.repeat(valueWidth + 2)}`];
        columns.forEach((col, i) => lines.push(` ${col.name.padEnd(nameWidth)} | ${values[i]}`));
        return lines.join('\n');
    });
    return blocks.join('\n\n');
}

function formatSchemaChange(change) {
    const name = [change.keyspace, ...change.arguments.slice(0, 1)].join('.');
    return `${change.change} ${change.target} ${name}`;
}

// Show the result of a query, as returned by the server.
function printResult(json) {
    for (const warning of json.warnings ?? []) {
        printNotice(`Warning: ${warning}`);
    }

    if (state.options.output_format === 'JSON') {
        printOutput(JSON.stringify(json, null, 2));
        return;
    }

    if (json.result_set) {
        if (state.options.expand) {
            if (json.result_set.length > 0) {
                printOutput(formatExpanded(json.column_types, json.result_set, state.pagingRows + 1));
            }
        } else {
            printOutput(formatTable(json.column_types, json.result_set));
        }
        const total = state.pagingRows + json.result_set.length;
        if (!json.paging_state) {
            printOutput(`\n(${total} row${total === 1 ? '' : 's'})`);
        }
    } else if (json.schema_change) {
        printOutput(formatSchemaChange(json.schema_change));
    }
}

/**
 * Input state
 */

function activeInput() {
    return passwordEl.hidden ? inputEl : passwordEl;
}

function autoGrow() {
    inputEl.style.height = 'auto';
    inputEl.style.height = `${inputEl.scrollHeight}px`;
}

function setInputValue(value) {
    inputEl.value = value;
    autoGrow();
    inputEl.setSelectionRange(value.length, value.length);
    scrollToBottom();
}

// Hiding the row blurs whatever inside it had focus, which would leave the
// keyboard pointing at nothing while a query runs or a page waits to be
// confirmed. Focus moves to the terminal itself so that PageUp/PageDown still
// scroll the scrollback and the document-level keydown handler still has a
// sensible target.
function hideRow() {
    rowEl.hidden = true;
    screenEl.focus({ preventScroll: true });
}

function showRow(phase) {
    state.phase = phase;

    const password = phase === Phase.LoginPassword;
    inputEl.hidden = password;
    passwordEl.hidden = !password;
    inputEl.readOnly = false;
    rowMarkEl.textContent = '';
    rowEl.hidden = false;

    refreshPromptEl();

    if (password) {
        passwordEl.value = '';
        passwordEl.focus();
    } else {
        setInputValue('');
        inputEl.focus();
    }

    scrollToBottom();
}

function ready() {
    showRow(Phase.Ready);
}

function beginBusy(label) {
    state.phase = Phase.Busy;
    hideRow();
    state.busyBlock = printBlock('busy', label, { mark: '▪' });
}

function endBusy() {
    if (state.busyBlock) {
        state.busyBlock.remove();
        state.busyBlock = null;
    }
}

/**
 * History
 */

function pushHistory(entry) {
    if (entry !== '' && entry !== state.history[state.history.length - 1]) {
        state.history.push(entry);
    }
    state.historyIndex = null;
    state.historyDraft = '';
}

function cycleHistory(direction) {
    if (state.history.length === 0) {
        return;
    }

    if (state.historyIndex === null) {
        if (direction > 0) {
            return; // Already at the newest entry.
        }
        state.historyDraft = inputEl.value;
        state.historyIndex = state.history.length;
    }

    const next = state.historyIndex + direction;
    if (next < 0) {
        return;
    }
    if (next >= state.history.length) {
        state.historyIndex = null;
        setInputValue(state.historyDraft);
        return;
    }

    state.historyIndex = next;
    setInputValue(state.history[next]);
}

/**
 * Completion
 */

let charWidth = 0;

function measureCharWidth() {
    const probe = document.createElement('span');
    probe.style.cssText = 'position:absolute;visibility:hidden;white-space:pre;';
    probe.textContent = '0'.repeat(100);
    scrollbackEl.appendChild(probe);
    charWidth = probe.getBoundingClientRect().width / 100;
    probe.remove();
}

function screenColumns() {
    if (!(charWidth > 0)) {
        measureCharWidth();
    }
    // The measurement is unusable before the font has loaded or while the
    // terminal is hidden; fall back to a conventional width rather than
    // producing a NaN column count.
    if (!(charWidth > 0) || !(screenEl.clientWidth > 0)) {
        return 80;
    }
    // Subtract the gutter (3ch) and the padding on both sides (2ch each).
    return Math.max(16, Math.floor(screenEl.clientWidth / charWidth) - 7);
}

function commonPrefix(values) {
    let prefix = values[0];
    for (const value of values.slice(1)) {
        let i = 0;
        while (i < prefix.length && i < value.length
               && prefix[i].toLowerCase() === value[i].toLowerCase()) {
            i++;
        }
        prefix = prefix.slice(0, i);
    }
    return prefix;
}

// Prints candidates the way a shell does, in aligned columns, rather than in a
// popup: the scrollback is already the right surface for a list of words.
function printCandidates(values) {
    const width = Math.max(...values.map((v) => v.length)) + 2;
    const perLine = Math.max(1, Math.floor(screenColumns() / width));
    const lines = [];

    for (let i = 0; i < values.length; i += perLine) {
        lines.push(values.slice(i, i + perLine).map((v) => v.padEnd(width)).join('').trimEnd());
    }

    echoInput(inputEl.value);
    printOutput(lines.join('\n'));
}

function matchDirective(text) {
    const lower = text.trim().toLowerCase();
    for (const directive of DIRECTIVES) {
        if (!lower.startsWith(directive.name)) {
            continue;
        }
        const next = lower[directive.name.length];
        if (next === undefined || /[^a-z0-9_]/.test(next)) {
            return directive;
        }
    }
    return null;
}

// Works out what is being completed and over which range of the input, so that
// multi-word directive names ("serial consistency") can replace the whole
// statement while ordinary words replace just the token under the cursor.
function completionAt(value, pos) {
    const lineStart = value.lastIndexOf('\n', pos - 1) + 1;
    const indent = /^\s*/.exec(value.slice(lineStart, pos))[0].length;
    const stmtStart = lineStart + indent;
    const stmt = value.slice(stmtStart, pos);
    const token = /[A-Za-z0-9_."]*$/.exec(stmt)[0];
    const tokenStart = pos - token.length;

    // Typing the arguments of a directive: complete against its values.
    const directive = matchDirective(stmt);
    if (directive && stmt.length > directive.name.length) {
        const values = OPTIONS[directive.name] ?? COMMANDS[directive.name] ?? [];
        return { start: tokenStart, end: pos, token, candidates: values };
    }

    // Part way through a directive name that spans a space ("output fo",
    // "show "). Only this case needs to replace more than the token under the
    // cursor, because the candidate itself contains a space.
    if (/\s/.test(stmt)) {
        const names = DIRECTIVE_NAMES.filter((n) => n.startsWith(stmt.toLowerCase()));
        if (names.length > 0) {
            return { start: stmtStart, end: pos, token: stmt, candidates: names };
        }
    }

    // Anywhere else: statement starters at the beginning of a line, CQL
    // keywords once a statement is under way. A single-word statement is still
    // its own token, so directive names and CQL keywords compete here on equal
    // terms -- "s" has to be able to reach SELECT as well as SHOW SESSION.
    const candidates = stmt === token ? STATEMENT_STARTS : CQL_KEYWORDS;
    return { start: tokenStart, end: pos, token, candidates };
}

function complete() {
    const value = inputEl.value;
    const pos = inputEl.selectionStart;
    const { start, end, token, candidates } = completionAt(value, pos);

    const lower = token.toLowerCase();
    const matches = candidates.filter((c) => c.toLowerCase().startsWith(lower));
    if (matches.length === 0) {
        return;
    }

    const prefix = commonPrefix(matches);
    const insertion = matches.length === 1 ? `${matches[0]} ` : prefix;

    if (insertion.length > token.length) {
        const next = value.slice(0, start) + insertion + value.slice(end);
        inputEl.value = next;
        autoGrow();
        const caret = start + insertion.length;
        inputEl.setSelectionRange(caret, caret);
    } else if (matches.length > 1) {
        printCandidates(matches);
    }

    scrollToBottom();
}

/**
 * Submitting
 */

// Enter submits a statement only once it is complete, so multi-line CQL can be
// typed the way cqlsh accepts it.
function isComplete(text) {
    const trimmed = text.trim();
    if (trimmed === '') {
        return true;
    }
    if (BUILTINS.includes(trimmed.toLowerCase())) {
        return true;
    }
    if (matchDirective(trimmed)) {
        return true;
    }
    // A ';' inside a string literal, a quoted identifier or a $$-quoted string
    // doesn't end the statement, nor does one with any of those left open.
    const unquoted = trimmed.replace(/'(?:[^']|'')*'|"(?:[^"]|"")*"|\$\$[\s\S]*?\$\$/g, '');
    if (/['"]|\$\$/.test(unquoted)) {
        return false;
    }
    return unquoted.trimEnd().endsWith(';');
}

// SHOW SESSION <tracing-session-id>: the events of a tracing session, as a
// query, so it is paged like any other.
function showSessionQuery(args) {
    if (args.length !== 1 || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(args[0])) {
        return null;
    }
    return `SELECT activity, source, source_elapsed, thread FROM system_traces.events WHERE session_id = ${args[0]}`;
}

async function runDirective(directive, text) {
    // Statements end in ';', directives may too.
    const args = text.trim()
        .replace(/;+\s*$/, '')
        .slice(directive.name.length)
        .split(/\s+/)
        .filter((arg) => arg !== '');

    if (directive.key === 'option') {
        const error = runOption(directive.name, args);
        if (error) {
            printError(error);
        }
        ready();
        return;
    }

    if (directive.name === 'help') {
        printOutput(HELP_TEXT);
        ready();
        return;
    }

    // show session
    const query = showSessionQuery(args);
    if (!query) {
        printError('Invalid SHOW SESSION command, expected: SHOW SESSION <tracing-session-id>');
        ready();
        return;
    }
    await runQuery(query);
}

async function runQuery(text) {
    // The server takes a bare statement; the terminating semicolon is ours.
    const query = text.trim().replace(/;+\s*$/, '');
    const body = { query, ...queryOptions() };

    if (state.pagingState !== null && state.pagingQuery === query) {
        body.paging_state = state.pagingState;
    } else {
        state.pagingRows = 0;
    }

    beginBusy('running query');
    const response = await postQuery(body);
    endBusy();

    if (await handleAuthFailure(response)) {
        return;
    }

    if (!response.ok) {
        state.pagingState = null;
        state.pagingQuery = null;
        printError(response.message);
        ready();
        return;
    }

    const json = response.json;
    if (json.set_keyspace !== undefined) {
        state.keyspace = json.set_keyspace;
    }

    printResult(json);

    const traceId = json.trace_session_id;
    if (traceId) {
        printNotice(`Tracing session ${traceId}. Run SHOW SESSION ${traceId} for the events.`);
    }

    if (json.paging_state) {
        state.pagingState = json.paging_state;
        state.pagingQuery = query;
        state.pagingRows += json.result_set?.length ?? 0;
        showMorePrompt();
        return;
    }

    state.pagingState = null;
    state.pagingQuery = null;

    ready();
}

async function submit(text) {
    const trimmed = text.trim();

    echoInput(text);
    pushHistory(text.trim());
    hideRow();

    if (trimmed === '') {
        ready();
        return;
    }

    const builtin = trimmed.toLowerCase();
    if (builtin === 'clear') {
        scrollbackEl.replaceChildren();
        ready();
        return;
    }
    if (builtin === 'exit' || builtin === 'quit') {
        await logout();
        return;
    }

    const directive = matchDirective(trimmed);
    if (directive) {
        await runDirective(directive, trimmed);
    } else {
        await runQuery(text);
    }
}

/**
 * Paging
 */

function showMorePrompt() {
    state.phase = Phase.More;
    hideRow();
    state.moreBlock = printBlock('more', "-- more -- press 'c' to continue, 'q' to stop", { mark: '·' });
}

function clearMorePrompt() {
    if (state.moreBlock) {
        state.moreBlock.remove();
        state.moreBlock = null;
    }
}

async function continuePaging() {
    clearMorePrompt();
    await runQuery(state.pagingQuery);
}

function stopPaging() {
    clearMorePrompt();
    state.pagingState = null;
    state.pagingQuery = null;
    ready();
}

/**
 * Signing in and out
 */

function resetIdentity() {
    state.user = null;
    state.cluster = null;
    state.credentials = null;
    state.keyspace = null;
    state.pagingState = null;
    state.pagingQuery = null;
    identityEl.textContent = '';
    signoutEl.hidden = true;
    renderStatus();
}

// Each request is authenticated on its own, so the credentials can stop
// working at any time: the password was changed, or the role was dropped.
async function handleAuthFailure(response) {
    if (response.status !== 401) {
        return false;
    }

    resetIdentity();
    printError(`Authentication failed: ${response.message}`);
    await startSession();
    return true;
}

function onLoggedIn(user, credentials, json) {
    state.user = user;
    state.credentials = credentials;
    state.cluster = json?.result_set?.[0]?.cluster_name ?? null;
    state.keyspace = null;
    state.options = { ...DEFAULT_OPTIONS };
    state.history = [];
    state.pagingState = null;
    state.pagingQuery = null;

    identityEl.textContent = state.cluster ? `${state.user}@${state.cluster}` : state.user;
    signoutEl.hidden = false;
    renderStatus();

    printNotice(credentials ? `Signed in as ${user}.` : 'Signed in as the anonymous user.');
    printBlank();
    ready();
}

function promptLogin() {
    state.loginUser = null;
    showRow(Phase.LoginUser);
}

// Signing in checks the credentials, which are then sent with every request.
async function doLogin(username, password) {
    const credentials = basicCredentials(username, password);

    beginBusy('authenticating');
    const response = await postQuery({ query: PROBE_QUERY }, credentials);
    endBusy();

    if (response.ok) {
        onLoggedIn(username, credentials, response.json);
        return;
    }

    printError(response.status === 401 ? `Login failed: ${response.message}` : response.message);
    promptLogin();
}

// A request without credentials succeeds if the cluster allows anonymous
// access; a rejection means it wants credentials.
async function startSession() {
    beginBusy('connecting');
    const response = await postQuery({ query: PROBE_QUERY }, null);
    endBusy();

    if (response.ok) {
        onLoggedIn('anonymous', null, response.json);
    } else if (response.status === 401) {
        promptLogin();
    } else {
        printError(response.message);
        printNotice('Reload the page to retry.');
    }
}

async function logout() {
    resetIdentity();
    printNotice('Signed out.');
    printBlank();

    await startSession();
}

/**
 * Key handling
 */

function caretOnFirstLine() {
    return inputEl.value.lastIndexOf('\n', inputEl.selectionStart - 1) === -1;
}

function caretOnLastLine() {
    return inputEl.value.indexOf('\n', inputEl.selectionStart) === -1;
}

const SCROLL_KEYS = ['PageUp', 'PageDown', 'Home', 'End', 'ArrowUp', 'ArrowDown'];

// On the document rather than on the input: the phases that answer a single
// keystroke -- paging, and waiting out a query -- are exactly the phases where
// the input row is hidden and therefore cannot hold focus.
document.addEventListener('keydown', (event) => {
    if (state.phase === Phase.More) {
        // Scrolling the result that is being paged, and the browser's own
        // shortcuts (copy above all), have to keep working.
        if (SCROLL_KEYS.includes(event.key) || event.ctrlKey || event.metaKey || event.altKey) {
            return;
        }
        event.preventDefault();
        if (event.key === 'c' || event.key === ' ' || event.key === 'Enter') {
            continuePaging();
        } else if (event.key === 'q' || event.key === 'Escape') {
            stopPaging();
        }
        return;
    }

    if (state.phase === Phase.Busy) {
        if (event.target === inputEl) {
            event.preventDefault();
        }
        return;
    }

    // Past this point everything acts on the textarea, so it has to be the
    // thing being typed into. The password field has its own handler.
    if (event.target !== inputEl) {
        // Except that focus may be sitting on the terminal, which is focusable
        // so paging keys have a target, or left there by selecting output.
        // Typing a character should reach the prompt, as it would in a
        // terminal. The sign-out button keeps its own keys.
        if (state.phase === Phase.Ready && !rowEl.hidden && !inputEl.hidden
                && event.target !== signoutEl
                && event.key.length === 1
                && !event.ctrlKey && !event.metaKey && !event.altKey) {
            inputEl.focus({ preventScroll: true });
        }
        return;
    }

    // Ctrl+C with nothing selected abandons the line, as in a shell. With a
    // selection it stays a copy.
    if (event.ctrlKey && event.key === 'c' && window.getSelection().isCollapsed) {
        event.preventDefault();
        echoInput(inputEl.value);
        setInputValue('');
        state.historyIndex = null;
        return;
    }

    if (event.ctrlKey && event.key === 'l') {
        event.preventDefault();
        scrollbackEl.replaceChildren();
        return;
    }

    // Tab completes, so it cannot also move focus. That is right for a shell,
    // but focus still has to be able to leave the input, or the sign-out button
    // cannot be reached with a keyboard at all. Shift+Tab is deliberately left
    // to the browser as that way out: the sign-out button is the only tab stop
    // before the input, so reverse-tabbing lands exactly there. Tab is claimed
    // only when there is completion to offer, which also leaves it tabbing
    // normally during the login prompts, where it would otherwise be a dead key.
    if (event.key === 'Tab' && !event.shiftKey && state.phase === Phase.Ready) {
        event.preventDefault();
        complete();
        return;
    }

    if (event.key === 'Enter') {
        if (event.shiftKey) {
            return; // Newline.
        }

        event.preventDefault();

        if (state.phase === Phase.LoginUser) {
            state.loginUser = inputEl.value.trim();
            echoInput(state.loginUser);
            showRow(Phase.LoginPassword);
            return;
        }

        if (!event.ctrlKey && !event.metaKey && !isComplete(inputEl.value)) {
            const pos = inputEl.selectionStart;
            inputEl.value = `${inputEl.value.slice(0, pos)}\n${inputEl.value.slice(pos)}`;
            inputEl.setSelectionRange(pos + 1, pos + 1);
            autoGrow();
            scrollToBottom();
            return;
        }

        submit(inputEl.value);
        return;
    }

    if (event.key === 'ArrowUp' && caretOnFirstLine()) {
        event.preventDefault();
        cycleHistory(-1);
        return;
    }

    if (event.key === 'ArrowDown' && caretOnLastLine()) {
        event.preventDefault();
        cycleHistory(+1);
    }
});

passwordEl.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter') {
        return;
    }
    event.preventDefault();

    const password = passwordEl.value;
    passwordEl.value = '';
    echoInput('');
    hideRow();
    doLogin(state.loginUser, password);
});

inputEl.addEventListener('input', autoGrow);

/**
 * Wiring
 */

screenEl.addEventListener('click', () => {
    // Clicking to place the caret should not steal a selection the user just
    // made in the scrollback.
    if (window.getSelection().isCollapsed && !rowEl.hidden) {
        activeInput().focus();
    }
});

signoutEl.addEventListener('click', () => {
    if (state.phase === Phase.Ready) {
        logout();
    }
});

window.addEventListener('resize', measureCharWidth);

/**
 * Main
 */

measureCharWidth();
renderStatus();
printBanner();
startSession();
