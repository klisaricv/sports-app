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
  // At least 8 characters, 1 uppercase, 1 lowercase, 1 number, 1 special character
  const re = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$/;
  return re.test(password);
}

function showError(message, title = "Error") {
  // Create custom error modal
  const modal = document.createElement('div');
  modal.id = 'customModalOverlay';
  modal.style.display = 'flex';
  modal.innerHTML = `
    <div id="customModal">
      <div id="customModalTitle">${title}</div>
      <div id="customModalMessage">${message}</div>
      <div id="customModalButtons">
        <button class="customModalBtn primary" onclick="this.closest('#customModalOverlay').remove()">OK</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
}

function showFieldError(inputId, message) {
  // Remove existing error for this field
  clearFieldError(inputId);
  
  const input = document.getElementById(inputId);
  if (!input) return;
  
  const errorDiv = document.createElement('div');
  errorDiv.className = 'field-error';
  errorDiv.textContent = message;
  
  // Insert after the input wrapper
  const inputWrapper = input.closest('.form-input-wrapper') || input.closest('.form-group');
  if (inputWrapper) {
    inputWrapper.parentNode.insertBefore(errorDiv, inputWrapper.nextSibling);
  }
  
  // Add error class to input
  input.classList.add('error');
}

function clearFieldError(inputId) {
  const input = document.getElementById(inputId);
  if (!input) return;
  
  // Remove error class
  input.classList.remove('error');
  
  // Remove existing error message
  const existingError = input.closest('.form-group')?.querySelector('.field-error');
  if (existingError) {
    existingError.remove();
  }
}

function clearAllFieldErrors() {
  const errors = document.querySelectorAll('.field-error');
  errors.forEach(error => error.remove());
  
  const errorInputs = document.querySelectorAll('.form-input.error');
  errorInputs.forEach(input => input.classList.remove('error'));
  
  // Clear terms checkbox error
  const termsWrapper = document.querySelector('.checkbox-wrapper');
  if (termsWrapper) {
    const termsError = termsWrapper.parentNode.querySelector('.field-error');
    if (termsError) {
      termsError.remove();
    }
  }
}

function resetForm(form, clearInputs = false) {
  // Clear all field errors
  clearAllFieldErrors();
  
  // Reset form inputs only if requested
  if (clearInputs) {
    const inputs = form.querySelectorAll('input');
    inputs.forEach(input => {
      if (input.type === 'checkbox') {
        input.checked = false;
      } else {
        input.value = '';
      }
    });
  }
  
  // Reset button
  const submitBtn = form.querySelector('.auth-btn');
  if (submitBtn) {
    removeLoadingAnimation(submitBtn);
    submitBtn.disabled = false;
    
    // Reset button text based on form type
    if (form.id === 'loginForm') {
      submitBtn.innerHTML = `
        <span>Sign In</span>
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M5 12H19" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M12 5L19 12L12 19" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
      `;
    } else if (form.id === 'registerForm') {
      submitBtn.innerHTML = `
        <span>Create Account</span>
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M16 21V19C16 17.9391 15.5786 16.9217 14.8284 16.1716C14.0783 15.4214 13.0609 15 12 15H5C3.93913 15 2.92172 15.4214 2.17157 16.1716C1.42143 16.9217 1 17.9391 1 19V21" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M8.5 11C10.7091 11 12.5 9.20914 12.5 7C12.5 4.79086 10.7091 3 8.5 3C6.29086 3 4.5 4.79086 4.5 7C4.5 9.20914 6.29086 11 8.5 11Z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M20 8V14" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M17 11H23" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
      `;
    }
  }
}

function addLoadingAnimation(button) {
  button.classList.add('loading');
  button.style.position = 'relative';
  button.style.overflow = 'hidden';
  
  // Add ripple effect
  const ripple = document.createElement('div');
  ripple.className = 'ripple-effect';
  button.appendChild(ripple);
  
  // Trigger ripple animation
  setTimeout(() => {
    ripple.style.animation = 'ripple 0.6s ease-out';
  }, 10);
}

function removeLoadingAnimation(button) {
  button.classList.remove('loading');
  const ripple = button.querySelector('.ripple-effect');
  if (ripple) {
    ripple.remove();
  }
}

function showSuccess(message, title = "Success") {
  // Create custom success modal
  const modal = document.createElement('div');
  modal.id = 'customModalOverlay';
  modal.style.display = 'flex';
  modal.innerHTML = `
    <div id="customModal">
      <div id="customModalTitle">${title}</div>
      <div id="customModalMessage">${message}</div>
      <div id="customModalButtons">
        <button class="customModalBtn primary" onclick="this.closest('#customModalOverlay').remove()">OK</button>
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
  
  // Clear previous errors
  clearAllFieldErrors();
  
  // Validation
  let hasErrors = false;
  
  if (!validateEmail(email)) {
    showFieldError('email', 'Please enter a valid email address.');
    hasErrors = true;
  }
  
  if (!password || password.length < 6) {
    showFieldError('password', 'Password must be at least 6 characters long.');
    hasErrors = true;
  }
  
  if (hasErrors) return;
  
  // Show loading state with animation
  const submitBtn = form.querySelector('.auth-btn');
  const originalText = submitBtn.innerHTML;
  
  addLoadingAnimation(submitBtn);
  submitBtn.innerHTML = `
    <span>Signing In...</span>
    <div style="display: inline-block; width: 16px; height: 16px; margin-left: 8px;">
      <div style="width: 100%; height: 100%; border: 2px solid rgba(255,255,255,0.3); border-top: 2px solid white; border-radius: 50%; animation: spin 1s linear infinite;"></div>
    </div>
  `;
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
      
      // Show success animation and redirect
      submitBtn.innerHTML = `
        <span>Success!</span>
        <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" style="margin-left: 8px;">
          <path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/>
        </svg>
      `;
      
      setTimeout(() => {
        window.location.href = '/';
      }, 1000);
    } else {
      if (data.message && (data.message.includes('Invalid') || data.message.includes('incorrect'))) {
        showFieldError('password', 'Invalid email or password. Please check your credentials and try again.');
      } else {
        showError(data.message || "Login failed. Please try again.");
      }
      // Reset form after error (don't clear inputs for field errors)
      setTimeout(() => {
        resetForm(form, false);
      }, 2000);
    }
  } catch (error) {
    console.error('Login error:', error);
    showError("Login failed. Please check your connection and try again.");
    // Reset form after error (clear inputs for connection errors)
    setTimeout(() => {
      resetForm(form, true);
    }, 2000);
  } finally {
    // Reset button only if not successful
    if (!data?.success) {
      removeLoadingAnimation(submitBtn);
      submitBtn.innerHTML = originalText;
      submitBtn.disabled = false;
    }
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
  
  // Clear previous errors
  clearAllFieldErrors();
  
  // Validation
  let hasErrors = false;
  
  if (!firstName || !lastName) {
    if (!firstName) showFieldError('firstName', 'Please enter your first name.');
    if (!lastName) showFieldError('lastName', 'Please enter your last name.');
    hasErrors = true;
  }
  
  if (!validateEmail(email)) {
    showFieldError('email', 'Please enter a valid email address.');
    hasErrors = true;
  }
  
  if (!validatePassword(password)) {
    showFieldError('password', 'Password must be at least 8 characters long and contain at least one uppercase letter, one lowercase letter, one number, and one special character (@$!%*?&).');
    hasErrors = true;
  }
  
  if (password !== confirmPassword) {
    showFieldError('confirmPassword', 'Passwords do not match.');
    hasErrors = true;
  }
  
  console.log('agreeTerms value:', agreeTerms); // Debug log
  if (!agreeTerms || agreeTerms !== 'on') {
    // For checkbox, we need to show error differently
    const termsWrapper = document.querySelector('.checkbox-wrapper');
    if (termsWrapper) {
      // Remove existing error first
      const existingError = termsWrapper.parentNode.querySelector('.field-error');
      if (existingError) {
        existingError.remove();
      }
      
      const errorDiv = document.createElement('div');
      errorDiv.className = 'field-error';
      errorDiv.textContent = 'Please agree to the Terms of Service and Privacy Policy.';
      termsWrapper.parentNode.insertBefore(errorDiv, termsWrapper.nextSibling);
    }
    hasErrors = true;
  }
  
  console.log('hasErrors:', hasErrors); // Debug log
  if (hasErrors) return;
  
  // Show loading state with animation
  const submitBtn = form.querySelector('.auth-btn');
  const originalText = submitBtn.innerHTML;
  
  addLoadingAnimation(submitBtn);
  submitBtn.innerHTML = `
    <span>Creating Account...</span>
    <div style="display: inline-block; width: 16px; height: 16px; margin-left: 8px;">
      <div style="width: 100%; height: 100%; border: 2px solid rgba(255,255,255,0.3); border-top: 2px solid white; border-radius: 50%; animation: spin 1s linear infinite;"></div>
    </div>
  `;
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
      // Show success animation
      submitBtn.innerHTML = `
        <span>Account Created!</span>
        <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" style="margin-left: 8px;">
          <path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/>
        </svg>
      `;
      
      // Show welcome message
      showSuccess(`Welcome to Sports Analysis, ${firstName}! Your account has been created successfully. You can now sign in with your credentials and start analyzing sports data.`, "Welcome to Sports Analysis!");
      
      // Redirect to login after user closes modal
      const modal = document.getElementById('customModalOverlay');
      if (modal) {
        modal.addEventListener('click', (e) => {
          if (e.target === modal || e.target.classList.contains('customModalBtn')) {
            setTimeout(() => {
              window.location.href = '/login';
            }, 500);
          }
        });
      }
    } else {
      if (data.message && data.message.includes('already exists')) {
        showFieldError('email', 'An account with this email address already exists. Please try logging in instead.');
      } else {
        showError(data.message || "Registration failed. Please try again.");
      }
      // Reset form after error (don't clear inputs for field errors)
      setTimeout(() => {
        resetForm(form, false);
      }, 2000);
    }
  } catch (error) {
    console.error('Registration error:', error);
    showError("Registration failed. Please check your connection and try again.");
    // Reset form after error (clear inputs for connection errors)
    setTimeout(() => {
      resetForm(form, true);
    }, 2000);
  } finally {
    // Reset button only if not successful
    if (!data?.success) {
      removeLoadingAnimation(submitBtn);
      submitBtn.innerHTML = originalText;
      submitBtn.disabled = false;
    }
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

// ====== LOGO CLICK FUNCTIONALITY ======
function initLogoClick() {
  const logo = document.querySelector('.auth-logo');
  if (logo) {
    logo.style.cursor = 'pointer';
    logo.addEventListener('click', () => {
      window.location.href = '/';
    });
  }
}

// ====== INITIALIZATION ======
document.addEventListener('DOMContentLoaded', () => {
  // Initialize theme
  initThemeToggle();
  
  // Initialize password toggles
  initPasswordToggle();
  
  // Initialize logo click
  initLogoClick();
  
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
