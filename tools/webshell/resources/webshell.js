/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

'use strict';

/**
 * Constants
 */

const Endpoint = {
    Command: '/command',
    Login: '/login',
    Logout: '/logout',
    Option: '/option',
    Query: '/query',
};

const Phase = {
    LoginUser: 'login-user',
    LoginPassword: 'login-password',
    Ready: 'ready',
    Busy: 'busy',
    More: 'more',
};

// Session options handled by /option, and the values each one accepts. Kept in
// sync with option_handler in tools/webshell/webshell.cc.
const OPTIONS = {
    'consistency': ['ANY', 'ONE', 'TWO', 'THREE', 'QUORUM', 'ALL', 'LOCAL_QUORUM', 'EACH_QUORUM',
                    'SERIAL', 'LOCAL_SERIAL', 'LOCAL_ONE'],
    'expand': ['ON', 'OFF'],
    'output format': ['TEXT', 'JSON'],
    'paging': ['ON', 'OFF'],
    'serial consistency': ['SERIAL', 'LOCAL_SERIAL'],
    'tracing': ['ON', 'OFF'],
};

// Commands handled by /command, and the values each one accepts.
const COMMANDS = {
    'help': [],
    'show session': [],
};

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
    ...Object.keys(OPTIONS).map((name) => ({ name, endpoint: Endpoint.Option, key: 'option' })),
    ...Object.keys(COMMANDS).map((name) => ({ name, endpoint: Endpoint.Command, key: 'command' })),
].sort((a, b) => b.name.length - a.name.length);

const DIRECTIVE_NAMES = DIRECTIVES.map((d) => d.name).sort();

// Offered when completing at the start of a statement.
const STATEMENT_STARTS = [...DIRECTIVE_NAMES, ...BUILTINS, ...CQL_KEYWORDS].sort();

// Server-side defaults, see session_options in tools/webshell/webshell.cc. Keyed
// by the member names /option reports, so a report can be assigned straight in.
const DEFAULT_OPTIONS = {
    consistency: 'ONE',
    expand: false,
    output_format: 'TEXT',
    page_size: 100,
    serial_consistency: 'SERIAL',
    tracing: false,
};

// /option answers with values, not sentences, so wording them is up to us. Each
// one echoes the statement that would put the option where it now stands, which
// reads the same whether the option was just changed or only asked about - and
// can be typed back verbatim to restore the value later. Keyed by the member
// names /option reports, which are not always the name the statement uses.
const OPTION_ECHO = {
    consistency: (v) => `CONSISTENCY ${v}`,
    expand: (v) => `EXPAND ${v ? 'ON' : 'OFF'}`,
    output_format: (v) => `OUTPUT FORMAT ${v}`,
    page_size: (v) => `PAGING ${v > 0 ? v : 'OFF'}`,
    serial_consistency: (v) => `SERIAL CONSISTENCY ${v}`,
    tracing: (v) => `TRACING ${v ? 'ON' : 'OFF'}`,
};

const DOCS_URL = 'https://docs.scylladb.com/manual/master/operating-scylla/admin-tools/webshell.html';

/**
 * State
 */

const state = {
    phase: Phase.LoginUser,
    user: null,
    cluster: null,
    // The session options as the server last reported them: read in full at
    // login, and updated from the report every /option request answers with.
    options: { ...DEFAULT_OPTIONS },
    history: [],
    historyIndex: null,
    historyDraft: '',
    pagingState: null,
    pagingQuery: null,
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

// The response of /query is a JSON string for TEXT output, but raw JSON when
// the output format is JSON.
function asText(value) {
    if (value === undefined || value === null) {
        return '';
    }
    return typeof value === 'string' ? value : JSON.stringify(value, null, 2);
}

function printBanner() {
    const frag = document.createDocumentFragment();

    const title = document.createElement('b');
    title.textContent = 'ScyllaDB Web Shell';
    frag.append(title, '\n');
    frag.append('Experimental: behaviour and output may change between releases.\n');
    frag.append('Type HELP for commands and session options, or read the ');

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

        el.append(user, host, '> ');
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
        statusEl.appendChild(statusSegment('session', 'none', false));
        return;
    }

    const o = state.options;
    statusEl.append(
        statusSegment('cl', o.consistency, o.consistency !== DEFAULT_OPTIONS.consistency),
        statusSegment('serial', o.serial_consistency, o.serial_consistency !== DEFAULT_OPTIONS.serial_consistency),
        statusSegment('paging', o.page_size > 0 ? String(o.page_size) : 'off', o.page_size !== DEFAULT_OPTIONS.page_size),
        statusSegment('format', o.output_format.toLowerCase(), o.output_format !== DEFAULT_OPTIONS.output_format),
        statusSegment('expand', o.expand ? 'on' : 'off', o.expand),
        statusSegment('tracing', o.tracing ? 'on' : 'off', o.tracing),
    );
}

// Take the option values the server reported and show them. Every /option
// response is a report of the options it concerns, so the status bar follows
// what the server actually holds instead of what we believe we set.
function applyOptionReport(report) {
    Object.assign(state.options, report);
    renderStatus();

    // An option the client does not know about still gets printed, so a newer
    // server reporting more of them does not silently drop any.
    return Object.entries(report)
        .map(([member, value]) => OPTION_ECHO[member]?.(value) ?? `${member} ${value}`)
        .join('\n');
}

/**
 * Transport
 */

// Every endpoint answers with a JSON object carrying a "response" member, so
// one reader serves them all.
async function settleResponse(response) {
    let json = null;
    try {
        json = await response.json();
    } catch (e) {
        // Truncated or non-JSON body; fall back to the status line below.
    }

    return {
        ok: response.ok,
        status: response.status,
        json,
        message: asText(json?.response) || response.statusText || `HTTP ${response.status}`,
    };
}

async function request(endpoint, body) {
    let response;
    try {
        response = await fetch(endpoint, {
            method: 'POST',
            body: body === undefined ? undefined : JSON.stringify(body),
        });
    } catch (e) {
        return { ok: false, status: 0, message: `Cannot reach the server: ${e.message}` };
    }

    return settleResponse(response);
}

// Reading session options is a GET, keeping the endpoint's two halves apart:
// POST /option changes an option, GET /option only reports. With no name, every
// option is reported.
async function requestOptions(name) {
    const query = name === undefined ? '' : `?option=${encodeURIComponent(name)}`;

    let response;
    try {
        response = await fetch(`${Endpoint.Option}${query}`, { method: 'GET' });
    } catch (e) {
        return { ok: false, status: 0, message: `Cannot reach the server: ${e.message}` };
    }

    return settleResponse(response);
}

function getCookie(name) {
    for (const pair of document.cookie.split('; ')) {
        const eq = pair.indexOf('=');
        if (eq !== -1 && pair.slice(0, eq) === name) {
            return decodeURIComponent(pair.slice(eq + 1));
        }
    }
    return null;
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
    return trimmed.endsWith(';');
}

async function runDirective(directive, text) {
    const args = text.trim()
        .slice(directive.name.length)
        .split(/\s+/)
        .filter((arg) => arg !== '');

    // An option name with no value is a question, and questions are GETs.
    const reading = directive.key === 'option' && args.length === 0;

    beginBusy(reading ? 'reading option'
        : directive.key === 'option' ? 'setting option' : 'running command');

    const response = reading
        ? await requestOptions(directive.name)
        : await request(directive.endpoint, {
            [directive.key]: directive.name,
            arguments: args,
        });

    endBusy();

    if (await handleAuthFailure(response)) {
        return;
    }

    if (response.ok) {
        if (directive.key === 'option') {
            printOutput(applyOptionReport(response.json?.response ?? {}));
        } else {
            printOutput(asText(response.json?.response));
        }
    } else {
        printError(response.message);
    }

    ready();
}

async function runQuery(text) {
    // The server takes a bare statement; the terminating semicolon is ours.
    const query = text.trim().replace(/;+\s*$/, '');
    const body = { query };

    if (state.pagingState !== null && state.pagingQuery === query) {
        body.paging_state = state.pagingState;
    }

    beginBusy('running query');
    const response = await request(Endpoint.Query, body);
    endBusy();

    if (await handleAuthFailure(response)) {
        return;
    }

    if (!response.ok) {
        state.pagingState = null;
        printError(response.message);
        ready();
        return;
    }

    printOutput(asText(response.json?.response));

    const pagingState = response.json?.paging_state ?? null;
    if (pagingState) {
        state.pagingState = pagingState;
        state.pagingQuery = query;
        showMorePrompt();
        return;
    }

    state.pagingState = null;
    state.pagingQuery = null;

    const traceId = response.json?.trace_session_id;
    if (traceId) {
        printNotice(`Tracing session ${traceId}. Run SHOW SESSION ${traceId} for the events.`);
    }

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
 * Session lifecycle
 */

async function handleAuthFailure(response) {
    if (response.status !== 401) {
        return false;
    }

    state.user = null;
    state.cluster = null;
    identityEl.textContent = '';
    signoutEl.hidden = true;
    renderStatus();

    printNotice('Session expired.');
    await startSession();
    return true;
}

// Read every session option in one go. A reload mid-session re-attaches to the
// existing session (login answers "Already logged in"), whose options are
// whatever they were left at, so they have to be asked for rather than assumed.
// A failure here is not worth reporting or retrying: the defaults stay on show,
// and the next /option request corrects them.
async function loadOptions() {
    beginBusy('reading options');
    const response = await requestOptions();
    endBusy();

    if (response.ok && response.json?.response) {
        Object.assign(state.options, response.json.response);
    }

    renderStatus();
}

async function onLoggedIn(message) {
    state.user = getCookie('user_name') ?? 'anonymous';
    state.cluster = getCookie('cluster_name');
    state.options = { ...DEFAULT_OPTIONS };
    state.history = [];
    state.pagingState = null;
    state.pagingQuery = null;

    identityEl.textContent = state.cluster ? `${state.user}@${state.cluster}` : state.user;
    signoutEl.hidden = false;
    renderStatus();

    printNotice(message);
    await loadOptions();
    printBlank();
    ready();
}

function promptLogin() {
    state.loginUser = null;
    showRow(Phase.LoginUser);
}

async function doLogin(username, password) {
    beginBusy('authenticating');
    const response = await request(Endpoint.Login, { username, password });
    endBusy();

    if (response.ok) {
        await onLoggedIn(response.message);
        return;
    }

    printError(response.status === 400 ? `Login failed: ${response.message}` : response.message);
    promptLogin();
}

// An empty login request either resumes an existing session or starts an
// anonymous one; a rejection means the cluster wants credentials.
async function startSession() {
    beginBusy('connecting');
    const response = await request(Endpoint.Login);
    endBusy();

    if (response.ok) {
        await onLoggedIn(response.message);
    } else if (response.status === 0) {
        printError(response.message);
        printNotice('Reload the page to retry.');
    } else {
        promptLogin();
    }
}

async function logout() {
    beginBusy('signing out');
    const response = await request(Endpoint.Logout);
    endBusy();

    printNotice(response.message);

    state.user = null;
    state.cluster = null;
    identityEl.textContent = '';
    signoutEl.hidden = true;
    renderStatus();
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
