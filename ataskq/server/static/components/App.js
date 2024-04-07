import { render } from 'preact';
import { html } from 'htm/preact';

import Pagination from './Pagination.js';
import Content from './Content.js'
import NAV from './NAV.js';

render(html`
    <${NAV} />
    <${Content} />
    <${Pagination} />
    `,
    document.getElementById("root")
);
