import { page, currentPage, offset, limit } from '../signals'

const Pagination = () => {
  // Accessing .value in a component automatically re-renders when it changes:
  let value;
  let prevActive;
  let nextActive;

  if (page.value === 1) {
    value = [1, 2, 3];
    prevActive = false;
    nextActive = true;
  } else {
    value = [page.value - 1, page.value, page.value + 1];
    prevActive = true;
    nextActive = true;
  }

  const next = () => {
    console.log('next')
    // A signal is updated by assigning to the `.value` property:
    page.value++;
  }

  const prev = () => {
    // A signal is updated by assigning to the `.value` property:
    if (page.value > 1) {
      page.value--;
    }
  }

  const pageClick = (e) => {
    e.preventDefault();
    currentPage.value = Number(e.target.innerText);
    offset.value = (currentPage.value - 1) * limit;;
  }

  return (
    <nav aria-label="...">
      <ul class="pagination justify-content-center">
        <li class={`page-item ${!prevActive && 'disabled'}`} onClick={prev} ><a class="page-link" href="#" tabindex="-1">Previous</a></li>
        <li class={`page-item ${currentPage.value === value[0] && 'active'}`} onClick={pageClick}><a class="page-link" href="#">{value[0]}</a></li>
        <li class={`page-item ${currentPage.value === value[1] && 'active'}`} onClick={pageClick}><a class="page-link" href="#">{value[1]}</a></li>
        <li class={`page-item ${currentPage.value === value[2] && 'active'}`} onClick={pageClick}><a class="page-link" href="#">{value[2]}</a></li>
        <li class={`page-item ${!nextActive && 'disabled'}`} onClick={next}><a class="page-link" href="#">Next</a></li>
      </ul>
    </nav>
  )
}

export default Pagination;
