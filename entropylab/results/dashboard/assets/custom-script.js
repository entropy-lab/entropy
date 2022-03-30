window.setTimeout(() => {work()}, 1000);

function work() {
  //   th = document.querySelector('*[data-dash-column="success"] div');
  filter_headers = document.getElementsByClassName("dash-filter column-5");
  console.log(filter_headers.length);
  if (filter_headers.length != 1) {
    console.error("Couldn't find <th> element for success filter")
    return;
  }
  filter_header = filter_headers[0];
  console.log(filter_header);
  removeChildren(filter_header);
  new_filter = document.getElementById("success-filter");
  filter_header.appendChild(new_filter)
}

function removeChildren(element) {
  while (element.firstChild) {
    console.log("removing ", element.firstChild)
    element.removeChild(element.firstChild);
  }
}
