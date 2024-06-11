import { title, job_id } from '../signals'
import { useState, useEffect } from 'preact/hooks'

const Input = ({value, set}) => {
    const [val, setVal] = useState(value);

    useEffect(() => {
        setVal(value);
    }, [value])

    const handleOnChange = (ev) => {
        setVal(ev.target.value);
    }

    const handleOnKeyUp = (ev) => {
        if (ev.key == "Enter") {
            set(v)
        }
    }

    return (
        <input name='job_id' class="form-control me-2" type="text" placeholder="JOB ID" aria-label="JOB ID" value={val} onChange={handleOnChange} onKeyUp={handleOnKeyUp} />
    )
}

const NAV = () => {
    const onSubmit = (ev) => {
        // Prevent the default form submission behavior
        ev.preventDefault();
        const inputValue = new FormData(ev.target).get('job_id');
        const redirectUrl = ev.target.action + "/" + encodeURIComponent(inputValue);

        // Perform the redirect
        window.location.href = redirectUrl;
    }

    return (
        <nav id="navbar" class="navbar navbar-expand-lg bg-body-tertiary">
            <div class="container-fluid">
                <a class="navbar-brand" href="/custom_query/jobs_status">JOBS</a>
                <div class="navbar-brand">{title}</div>
                <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarScroll"
                    aria-controls="navbarScroll" aria-expanded="false" aria-label="Toggle navigation">
                    <span class="navbar-toggler-icon"></span>
                </button>
                <div class="collapse navbar-collapse" id="navbarScroll">
                    <ul class="navbar-nav me-auto my-2 my-lg-0 navbar-nav-scroll" style="--bs-scroll-height: 100px;">
                        <li class="nav-item">
                            <a class="nav-link active" aria-current="page" href={`/custom_query/tasks_status/${job_id}`}>Tasks Status</a>
                        </li>
                        <li class="nav-item">
                            <a class="nav-link active" aria-current="page" href={`/db/tasks/?job_id=${job_id}`}>Tasks</a>
                        </li>
                    </ul>
                    <form id="tasks-status-go" class="navbar-nav nav-form d-flex" action="/custom_query/tasks_status" method="GET" onSubmit={onSubmit}>
                        <Input value={job_id} set={(v) => job_id.value = v}/>
                        <button class="btn btn-outline-success" type="submit">Go</button>
                    </form>
                </div>
            </div>
        </nav>
    )
}

export default NAV
