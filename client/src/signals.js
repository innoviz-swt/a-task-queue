import { signal } from "@preact/signals";

// nav
export const title = signal(null);

// pagination
export const page = signal(1);
export const currentPage = signal(1);

// job_id
export const job_id = signal(null);

(function () {
    const { pathname, search, hash } = window.location;
    console.table({ pathname, search })
    // update job id \ title
    let my_job_id;
    let mytitle;
    if (pathname.startsWith("/custom_query/tasks_status/")) {
        my_job_id = pathname.split("/custom_query/tasks_status/")[1];
        mytitle = 'tasks_status - job ' + my_job_id
    }
    else {
        my_job_id = new URLSearchParams(search).get('job_id');
        mytitle = pathname.replace('/custom_query/', '').replace('/db/', '').replace('/', ' ')
        if (my_job_id) {
            mytitle += ` - job ${my_job_id}`
        }
    }
    job_id.value = my_job_id;

    // update title
    title.value = mytitle;

    // fetch table from api
    myfetch(pathname, search).catch(error => {
        if (Array.isArray(error) && error.length == 1) {
            error = error[0];
        }

        if (error && error.stack && error.message) {
            console.error(error);
        }
        else {
            error = JSON.stringify(error, null, 2);
        }

        // todo: replace with notification popup
        alert(`Fetch failed. Error: ${error}`);
    });

}());
