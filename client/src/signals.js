import { signal, effect, batch } from "@preact/signals";

// nav
export const title = signal(null);
export const offsetDefault = 0;
export const limitDefault = 100;
export const offset = signal(offsetDefault);
export const limit = signal(limitDefault);

// pagination
export const page = signal(1);
export const currentPage = signal(1);

// job_id
export const job_id = signal(null);

/*************/
/* init flow */
/*************/
const getColumn = (table, field) => {
    try {
        return table.getColumn(field);
    } catch (e) {
        return undefined;
    }
}

const init_table = (tabledata) => {
    //initialize table
    var table = new Tabulator("#table", {
        layout: "fitDataStretch",
        data: tabledata, //assign data to table
        autoColumns: true, //create columns from data field names
    });

    table.on("tableBuilt", () => {
        // edit job_id
        const job_id = getColumn(table, "job_id");
        if (job_id) {
            job_id.updateDefinition({ // Update the column definition
                formatter: "link",
                formatterParams: {
                    urlPrefix: "/custom_query/tasks_status/",
                }
            });
        }

        // edit targs
        const targs = getColumn(table, "targs");
        if (targs) {
            targs.updateDefinition({ // Update the column definition
                formatter: (cell, formatterParams, onRendered) => {
                    cell.getElement().classList.add("targs-cell");
                    return cell.getValue();
                }
            });
        }

    });
};

const myfetch = async (pathname, search) => {
    // handle /db/ prefix
    let url = pathname.replace('/db', '');
    url = '/api' + url;
    url = url + '?' + search;
    console.log('fetch: ', url)
    const response = await fetch(url);
    if (!response.ok) {
        const err = await response.json();
        throw err;
    }

    const data = await response.json();
    console.log('fetch', data);
    if (data){
        init_table(data);
    }

}

(function () {
    const { pathname, search, hash } = window.location;
    const searchParams = new URLSearchParams(search);
    console.table({ pathname, search })
    // update job id \ title
    let my_job_id;
    let mytitle;
    if (pathname.startsWith("/custom_query/tasks_status/")) {
        my_job_id = pathname.split("/custom_query/tasks_status/")[1];
        mytitle = 'tasks_status - job ' + my_job_id
    }
    else {
        my_job_id = searchParams.get('job_id');
        mytitle = pathname.replace('/custom_query/', '').replace('/db/', '').replace('/', ' ')
        if (my_job_id) {
            mytitle += ` - job ${my_job_id}`
        }
    }
    job_id.value = my_job_id;
    title.value = mytitle;

    // update pagination
    const mylimit = searchParams.get('_limit') || limitDefault;
    const myoffset = searchParams.get('_offset') || offsetDefault;
    batch(() => {
        limit.value = mylimit;
        offset.value = myoffset;
    });
}());

effect(() => {
    const { pathname, search, hash } = window.location;
    const searchParams = new URLSearchParams(search);

    searchParams.set('_offset', offset);
    searchParams.set('_limit', limit);

    // fetch table from api
    myfetch(pathname, searchParams.toString()).catch(error => {
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
})
