/*
The below code is commented out because it raises a client-side error in the Dashboard.

The error occurs because when the Dashboard app unloads the "Experiment Results" page,
React loops over components in the page, disposing of them. When React encounters the
new "success filter" control inside the experiments DataTable it raises an error.

At this time the new "success filter" functionality is disabled until the issue can be
resolved.
*/

/*
const selector_for_success_filter_container = ".dash-filter.column-5";
const id_of_new_success_filter = "success-filter-checklist";

waitForElm(selector_for_success_filter_container).then((elm) => {
    reposition_success_filter()
});

function reposition_success_filter() {
    container = document.querySelector(selector_for_success_filter_container);
    if (container === null) {
        console.error("Couldn't find 'success' filter container")
    } else {
        hideChildren(container);
        new_filter = document.getElementById(id_of_new_success_filter);
        if (new_filter === null) {
            console.error("Couldn't find new 'success' filter checklist")
        } else {
            container.appendChild(new_filter)
        }
    }
}

function hideChildren(element) {

    for (let i = 0; i < element.children.length; i++) {
        element.children[i].style.display = 'none';
    }
}

// Source: https://stackoverflow.com/a/61511955/33404
function waitForElm(selector) {
    return new Promise(resolve => {
        if (document.querySelector(selector)) {
            return resolve(document.querySelector(selector));
        }

        const observer = new MutationObserver(mutations => {
            if (document.querySelector(selector)) {
                resolve(document.querySelector(selector));
                observer.disconnect();
            }
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    });
}
*/
