import { render } from 'preact';
import { html } from 'htm/preact';

import Pagination from "./Pagination.js";

render(html`<${Pagination} />`, document.getElementById("pagination"));
