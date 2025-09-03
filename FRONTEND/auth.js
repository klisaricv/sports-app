// ====== AUTHENTICATION JAVASCRIPT ======

// ====== CONFIG ======
const BACKEND_URL = window.location.origin;

// ====== UTILITIES ======
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ====== FORM VALIDATION ======
function validateEmail(email) {
  const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return re.test(email);
}

function validatePassword(password) {
  // At least 8 characters, 1 uppercase, 1 lowercase, 1 number
  const re = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)[a-zA-Z\d@$!%*?&]{8,}$/;
  return re.test(password);
}

function showError(message, title = "Error") {
  // Create custom error modal
  const modal = document.createElement('div');
  modal.className = 'custom-modal-overlay';
  modal.innerHTML = `
    <div class="custom-modal">
      <div class="custom-modal-header">
        <h3 class="custom-modal-title">${title}</h3>
        <button class="custom-modal-close">&times;</button>
      </div>
      <div class="custom-modal-body">
        <p class="custom-modal-message">${message}</p>
      </div>
      <div class="custom-modal-footer">
        <button class="custom-modal-btn primary" onclick="this.closest('.custom-modal-overlay').remove()">OK</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
}

function showSuccess(message, title = "Success") {
  // Create custom success modal
  const modal = document.createElement('div');
  modal.className = 'custom-modal-overlay';
  modal.innerHTML = `
    <div class="custom-modal">
      <div class="custom-modal-header">
        <h3 class="custom-modal-title">${title}</h3>
        <button class="custom-modal-close">&times;</button>
      </div>
      <div class="custom-modal-body">
        <p class="custom-modal-message">${message}</p>
      </div>
      <div class="custom-modal-footer">
        <button class="custom-modal-btn primary" onclick="this.closest('.custom-modal-overlay').remove()">OK</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
}

// ====== PASSWORD TOGGLE ======
function initPasswordToggle() {
  const toggleButtons = document.querySelectorAll('.form-toggle');
  
  toggleButtons.forEach(button => {
    button.addEventListener('click', () => {
      const input = button.parentElement.querySelector('input');
      const isPassword = input.type === 'password';
      
      input.type = isPassword ? 'text' : 'password';
      
      // Update icon
      const icon = button.querySelector('svg');
      if (isPassword) {
        icon.innerHTML = `
          <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20C5 20 1 12 1 12A18.45 18.45 0 0 1 5.06 5.06L17.94 17.94Z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M9.9 4.24A9.12 9.12 0 0 1 12 4C19 4 23 12 23 12A18.5 18.5 0 0 1 19.94 18.94L9.9 4.24Z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M1 1L23 23" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        `;
      } else {
        icon.innerHTML = `
          <path d="M1 12S5 4 12 4S23 12 23 12S19 20 12 20S1 12 1 12Z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          <circle cx="12" cy="12" r="3" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        `;
      }
    });
  });
}

// ====== LOGIN FUNCTIONALITY ======
async function handleLogin(event) {
  event.preventDefault();
  
  const form = event.target;
  const formData = new FormData(form);
  const email = formData.get('email');
  const password = formData.get('password');
  const rememberMe = formData.get('rememberMe');
  
  // Validation
  if (!validateEmail(email)) {
    showError("Please enter a valid email address.");
    return;
  }
  
  if (!password || password.length < 6) {
    showError("Password must be at least 6 characters long.");
    return;
  }
  
  // Show loading state
  const submitBtn = form.querySelector('.auth-btn');
  const originalText = submitBtn.innerHTML;
  submitBtn.innerHTML = '<span>Signing In...</span>';
  submitBtn.disabled = true;
  
  try {
    // Call backend API
    const response = await fetch(`${BACKEND_URL}/api/auth/login`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        email: email,
        password: password,
        remember_me: !!rememberMe
      })
    });
    
    const data = await response.json();
    
    if (data.success) {
      showSuccess("Login successful! Redirecting...", "Welcome!");
      
      // Store user session
      localStorage.setItem('user', JSON.stringify({
        id: data.user.id,
        email: data.user.email,
        name: `${data.user.first_name} ${data.user.last_name}`,
        first_name: data.user.first_name,
        last_name: data.user.last_name,
        is_admin: data.user.is_admin,
        session_id: data.session_id,
        loginTime: new Date().toISOString()
      }));
      
      // Redirect to main app
      setTimeout(() => {
        window.location.href = '/';
      }, 1500);
    } else {
      showError(data.message || "Login failed. Please try again.");
    }
  } catch (error) {
    console.error('Login error:', error);
    showError("Login failed. Please check your connection and try again.");
  } finally {
    // Reset button
    submitBtn.innerHTML = originalText;
    submitBtn.disabled = false;
  }
}

// ====== REGISTER FUNCTIONALITY ======
async function handleRegister(event) {
  event.preventDefault();
  
  const form = event.target;
  const formData = new FormData(form);
  const firstName = formData.get('firstName');
  const lastName = formData.get('lastName');
  const email = formData.get('email');
  const password = formData.get('password');
  const confirmPassword = formData.get('confirmPassword');
  const agreeTerms = formData.get('agreeTerms');
  
  // Validation
  if (!firstName || !lastName) {
    showError("Please enter both first and last name.");
    return;
  }
  
  if (!validateEmail(email)) {
    showError("Please enter a valid email address.");
    return;
  }
  
  if (!validatePassword(password)) {
    showError("Password must be at least 8 characters long and contain at least one uppercase letter, one lowercase letter, and one number.");
    return;
  }
  
  if (password !== confirmPassword) {
    showError("Passwords do not match.");
    return;
  }
  
  if (!agreeTerms) {
    showError("Please agree to the Terms of Service and Privacy Policy.");
    return;
  }
  
  // Show loading state
  const submitBtn = form.querySelector('.auth-btn');
  const originalText = submitBtn.innerHTML;
  submitBtn.innerHTML = '<span>Creating Account...</span>';
  submitBtn.disabled = true;
  
  try {
    // Call backend API
    const response = await fetch(`${BACKEND_URL}/api/auth/register`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        email: email,
        password: password,
        first_name: firstName,
        last_name: lastName
      })
    });
    
    const data = await response.json();
    
    if (data.success) {
      showSuccess("Account created successfully! Redirecting to login...", "Welcome!");
      
      // Redirect to login
      setTimeout(() => {
        window.location.href = '/login';
      }, 1500);
    } else {
      showError(data.message || "Registration failed. Please try again.");
    }
  } catch (error) {
    console.error('Registration error:', error);
    showError("Registration failed. Please check your connection and try again.");
  } finally {
    // Reset button
    submitBtn.innerHTML = originalText;
    submitBtn.disabled = false;
  }
}

// ====== SOCIAL LOGIN ======
function handleGoogleLogin() {
  showError("Google login is not implemented yet. Please use email/password.", "Coming Soon");
}

// ====== THEME TOGGLE ======
function initThemeToggle() {
  // Check for saved theme preference or default to 'dark'
  const currentTheme = localStorage.getItem('theme') || 'dark';
  document.documentElement.setAttribute('data-theme', currentTheme);
}

// ====== INITIALIZATION ======
document.addEventListener('DOMContentLoaded', () => {
  // Initialize theme
  initThemeToggle();
  
  // Initialize password toggles
  initPasswordToggle();
  
  // Initialize forms
  const loginForm = document.getElementById('loginForm');
  const registerForm = document.getElementById('registerForm');
  
  if (loginForm) {
    loginForm.addEventListener('submit', handleLogin);
  }
  
  if (registerForm) {
    registerForm.addEventListener('submit', handleRegister);
  }
  
  // Initialize social login buttons
  const googleBtns = document.querySelectorAll('.social-btn.google');
  googleBtns.forEach(btn => {
    btn.addEventListener('click', handleGoogleLogin);
  });
  
  // Check if user is already logged in
  const user = localStorage.getItem('user');
  if (user && window.location.pathname.includes('login.html')) {
    // User is already logged in, redirect to main app
    window.location.href = 'index.html';
  }
});

// ====== EXPORT FOR TESTING ======
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    validateEmail,
    validatePassword,
    handleLogin,
    handleRegister
  };
}
