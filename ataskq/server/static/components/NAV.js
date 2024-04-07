import { html } from 'htm/preact';

const NAV = () => (html`
    <nav id="navbar" class="navbar navbar-expand-lg bg-body-tertiary">
        <div class="container-fluid">
            <a class="navbar-brand" href="/custom_query/jobs_status">JOBS</a>
            <div class="navbar-brand nv-title">Title</div>
            <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarScroll"
                aria-controls="navbarScroll" aria-expanded="false" aria-label="Toggle navigation">
                <span class="navbar-toggler-icon"></span>
            </button>
            <div class="collapse navbar-collapse" id="navbarScroll">
                <ul class="navbar-nav me-auto my-2 my-lg-0 navbar-nav-scroll" style="--bs-scroll-height: 100px;">
                    <li class="nav-item">
                        <a class="nav-link active add-job-id" aria-current="page" href="/db/tasks">Tasks</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link active add-job-id" aria-current="page" href="/db/state_kwargs">State
                            KWArgs</a>
                    </li>
                    <!-- <li class="nav-item">
                        <a class="nav-link" href="#">Link</a>
                    </li> -->
                    <!-- <li class="nav-item dropdown">
                        <a class="nav-link dropdown-toggle" href="#" role="button" data-bs-toggle="dropdown"
                            aria-expanded="false">
                            Link
                        </a>
                        <ul class="dropdown-menu">
                            <li><a class="dropdown-item" href="#">Action</a></li>
                            <li><a class="dropdown-item" href="#">Another action</a></li>
                            <li>
                                <hr class="dropdown-divider">
                            </li>
                            <li><a class="dropdown-item" href="#">Something else here</a></li>
                        </ul>
                    </li> -->
                    <!-- <li class="nav-item">
                        <a class="nav-link disabled" aria-disabled="true">Link</a>
                    </li> -->
                </ul>
                <form id="tasks-status-go" class="navbar-nav nav-form d-flex" action="/custom_query/tasks_status" method="GET">
                    <input id="tasks-status-go-input" class="form-control me-2" type="text" placeholder="JOB ID" aria-label="JOB ID"/>
                    <button class="btn btn-outline-success" type="submit">Go</button>
                </form>
            </div>
        </div>
    </nav>
    `
)

export default NAV
