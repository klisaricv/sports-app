// Users page specific functionality
let allUsers = [];
let filteredUsers = [];
let currentPage = 1;
const usersPerPage = 10;

// Check if user is admin
function checkAdminAccess() {
  const userEmail = localStorage.getItem('userEmail');
  if (userEmail !== 'klisaricf@gmail.com') {
    window.location.href = '/';
    return false;
  }
  return true;
}

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
  if (!checkAdminAccess()) return;
  
  // Show admin info
  const adminInfo = document.getElementById('adminInfo');
  const adminEmail = document.getElementById('adminEmail');
  const logoutBtn = document.getElementById('logoutBtn');
  
  adminInfo.style.display = 'flex';
  adminEmail.textContent = localStorage.getItem('userEmail');
  logoutBtn.style.display = 'flex';
  
  // Load users
  loadUsers();
  
  // Setup event listeners
  setupEventListeners();
});

// Setup event listeners
function setupEventListeners() {
  // Search functionality
  const searchInput = document.getElementById('userSearch');
  const clearSearch = document.getElementById('clearSearch');
  
  searchInput.addEventListener('input', handleSearch);
  clearSearch.addEventListener('click', clearSearchInput);
  
  // Pagination
  document.getElementById('prevPage').addEventListener('click', () => changePage(currentPage - 1));
  document.getElementById('nextPage').addEventListener('click', () => changePage(currentPage + 1));
  
  // Logout
  document.getElementById('logoutBtn').addEventListener('click', logout);
}

// Load users from API
async function loadUsers() {
  const loading = document.getElementById('tableLoading');
  const error = document.getElementById('tableError');
  const table = document.getElementById('usersTable');
  
  loading.style.display = 'flex';
  error.style.display = 'none';
  table.style.display = 'none';
  
  try {
    const response = await fetch('/api/users', {
      headers: {
        'Authorization': `Bearer ${localStorage.getItem('token')}`
      }
    });
    
    if (!response.ok) {
      throw new Error('Failed to load users');
    }
    
    const data = await response.json();
    allUsers = data.users || [];
    filteredUsers = [...allUsers];
    
    updateStats();
    renderUsers();
    renderPagination();
    
    loading.style.display = 'none';
    table.style.display = 'table';
    
  } catch (err) {
    console.error('Error loading users:', err);
    loading.style.display = 'none';
    error.style.display = 'flex';
    document.getElementById('errorMessage').textContent = err.message;
  }
}

// Handle search
function handleSearch(e) {
  const query = e.target.value.toLowerCase().trim();
  const clearBtn = document.getElementById('clearSearch');
  
  if (query) {
    clearBtn.style.display = 'flex';
    filteredUsers = allUsers.filter(user => 
      user.first_name.toLowerCase().includes(query) ||
      user.last_name.toLowerCase().includes(query) ||
      user.email.toLowerCase().includes(query)
    );
  } else {
    clearBtn.style.display = 'none';
    filteredUsers = [...allUsers];
  }
  
  currentPage = 1;
  updateStats();
  renderUsers();
  renderPagination();
}

// Clear search
function clearSearchInput() {
  document.getElementById('userSearch').value = '';
  document.getElementById('clearSearch').style.display = 'none';
  filteredUsers = [...allUsers];
  currentPage = 1;
  updateStats();
  renderUsers();
  renderPagination();
}

// Update statistics
function updateStats() {
  document.getElementById('totalUsers').textContent = allUsers.length;
  document.getElementById('showingUsers').textContent = filteredUsers.length;
}

// Render users table
function renderUsers() {
  const tbody = document.getElementById('usersTableBody');
  const noUsers = document.getElementById('noUsers');
  const pagination = document.getElementById('paginationContainer');
  
  if (filteredUsers.length === 0) {
    tbody.innerHTML = '';
    noUsers.style.display = 'flex';
    pagination.style.display = 'none';
    return;
  }
  
  noUsers.style.display = 'none';
  pagination.style.display = 'flex';
  
  const startIndex = (currentPage - 1) * usersPerPage;
  const endIndex = startIndex + usersPerPage;
  const pageUsers = filteredUsers.slice(startIndex, endIndex);
  
  tbody.innerHTML = pageUsers.map((user, index) => {
    const globalIndex = startIndex + index + 1;
    const registeredDate = new Date(user.created_at).toLocaleDateString('sr-RS');
    
    return `
      <tr>
        <td>${globalIndex}</td>
        <td>${user.first_name}</td>
        <td>${user.last_name}</td>
        <td>${user.email}</td>
        <td>${registeredDate}</td>
      </tr>
    `;
  }).join('');
}

// Render pagination
function renderPagination() {
  const totalPages = Math.ceil(filteredUsers.length / usersPerPage);
  const paginationInfo = document.getElementById('paginationInfo');
  const paginationPages = document.getElementById('paginationPages');
  const prevBtn = document.getElementById('prevPage');
  const nextBtn = document.getElementById('nextPage');
  
  if (totalPages <= 1) {
    document.getElementById('paginationContainer').style.display = 'none';
    return;
  }
  
  paginationInfo.textContent = `Page ${currentPage} of ${totalPages}`;
  
  // Previous button
  prevBtn.disabled = currentPage === 1;
  
  // Next button
  nextBtn.disabled = currentPage === totalPages;
  
  // Page numbers
  paginationPages.innerHTML = '';
  const maxVisiblePages = 5;
  let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
  let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
  
  if (endPage - startPage + 1 < maxVisiblePages) {
    startPage = Math.max(1, endPage - maxVisiblePages + 1);
  }
  
  for (let i = startPage; i <= endPage; i++) {
    const pageBtn = document.createElement('button');
    pageBtn.className = `btn pagination-page ${i === currentPage ? 'active' : ''}`;
    pageBtn.textContent = i;
    pageBtn.addEventListener('click', () => changePage(i));
    paginationPages.appendChild(pageBtn);
  }
}

// Change page
function changePage(page) {
  const totalPages = Math.ceil(filteredUsers.length / usersPerPage);
  if (page >= 1 && page <= totalPages) {
    currentPage = page;
    renderUsers();
    renderPagination();
  }
}

// Logout function
function logout() {
  localStorage.removeItem('token');
  localStorage.removeItem('userEmail');
  window.location.href = '/login';
}
