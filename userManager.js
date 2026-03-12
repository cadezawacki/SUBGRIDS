import FingerprintJS from '@fingerprintjs/fingerprintjs';
import Cookies from 'js-cookie';
import { Subject, Observable, BehaviorSubject, map, distinctUntilChanged, takeUntil} from 'rxjs';
import {debounce} from "@/utils/helpers.js";

export class UserManager {
    constructor(context) {
        this.context = context;

        // -- Setup Variables
        this.fp = null;
        this.session_fp = this.generateSessionFingerprint();
        this.context.session_fp = this.session_fp;

        this.onboardingAbortController = null; // For cleaning up onboarding listeners
        this.selectedUserData = null; // Store data from username selection
        this._samples = false;

        // -- Subscriptions
        this.userProfile$ = this.context.page.createSharedStore('userProfile', {},
            {persist: 'local', storageKey: 'userProfile'});
        this.isIdentitySet = false;

        this.avatar = null;
        this.displayName = null;
        this.email = null;
        this.fingerprint = null;
        this.firstName = null;
        this.lastName = null;
        this.nickname = null;
        this.region = null;
        this.role = null;
        this.username = null;

        this.setupSubscriptions();

        // -- Access Managers
        this.themeManger = this.context.themeManager;

        // Defaults
        this.selectedTheme = this.themeManger.getCurrentTheme();
        this.defaultAvatarType = 'bottts-neutral'; // Default avatar type
        this.selectedAvatarType = this.defaultAvatarType;
        this.selectedAvatarUrl = null; //this.generateAvatarUrl(this.selectedAvatarType); // Default avatar URL

        // Debounced function for username suggestions
        this.debouncedFetchUserSuggestions = debounce(
            this.fetchUserSuggestions.bind(this), 120);

        this.init();
    }

    setupSubscriptions() {
        this.isIdentitySet$ = this.userProfile$.subject.pipe(
            map(identity => {
                return (identity !== null && identity !== undefined)
            }),
            distinctUntilChanged()
        )
        this.isIdentitySet$.pipe(
            takeUntil(this.context.destroy$)
        ).subscribe(isSet => {
            this.isIdentitySet = isSet;
        });

        this.userProfile$.subject.pipe(
            takeUntil(this.context.destroy$)
        ).subscribe(identity => {
            Object.assign(this, Object.fromEntries(identity.entries()));
        })
    }

    user_data() {
        return {...this.userProfile$.selectKeys(['username', 'fingerprint']).asObject(), sessionFingerprint:this.session_fp}
    }

    init() {
        this.bindPersistentEvents();
    }

    async cleanup(){

    }

    generateSessionFingerprint() {
        return `fp_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
    }

    async initFingerprint() {

        try {

            this.fp = (await (await FingerprintJS.load()).get())
            const {screenFrame, screenResolution, ...components} = this.fp.components;

            if (this.userProfile$.get('username')) {

                const extendedComponents = {...components, username: {value: this.userProfile$.get('username')}};
                this.fp.visitorId = FingerprintJS.hashComponents(extendedComponents);

                if (this.fp.visitorId !== this.userProfile$.get('fingerprint')) {
                    console.warn("Fingerprint doesn't match stored profile! Updating fingerprint.");
                    this.userProfile$.set('fingerprint', this.fp.visitorId);
                }

                await this.finishLoading();

            } else {
                const newProfile = await this.startOnboardingProcess();

                if (newProfile) {
                    await this.finishLoading();
                } else {
                    console.error("Onboarding process was not completed.");
                }
            }
            return true;
        } catch (error) {
            console.error("Error during fingerprint initialization or onboarding:", error);
            return false;
        }
    }

    async applyUserSettings(changes) {
        if (changes.current && !changes.current.size) return;
        this.name = this.getCurrentUserName();
        await this.loadProfileDisplay(changes.current);
    }

    async finishLoading() {

        // Exit onboarding process - if open
        const modal = document.getElementById('new-user-modal');
        if (modal && modal.open) {
            modal.close();
        }
        this.cleanupOnboardingListeners();

        const ch = this.userProfile$.getValue();
        await this.applyUserSettings(ch)
        const cookieData = Object.fromEntries(ch);
        cookieData.sessionFingerprint = this.context.session_fp;
        this.setCookieData('userProfile', cookieData)

        // Start listening for user profile changes
        this.userProfile$.onChanges(async (ch) => {
            await this.applyUserSettings(ch)
            const cookieData = Object.fromEntries(ch.current.entries());
            cookieData.sessionFingerprint = this.context.session_fp;
            this.setCookieData('userProfile', cookieData)
        })


    }

    // --- Onboarding Process ---

    async startOnboardingProcess() {
        return new Promise((resolve, reject) => {

            const modal = document.getElementById('new-user-modal');
            if (!modal) return reject("Modal element not found");

            this.resetOnboardingState(); // Reset state variables
            this.resetForm(); // Clear form fields
            this.showScreen('username-screen'); // Start at the first screen

            // Use AbortController for cleanup
            this.onboardingAbortController = new AbortController();
            const signal = this.onboardingAbortController.signal;

            try {
                modal.showModal();

                // Wave Animation
                const wave = modal.querySelector('lord-icon[src="/assets/lottie/wave.json"]');
                if (wave) this.waitForLordIconReady(wave);

                // --- Bind Event Listeners SPECIFIC to this onboarding session ---
                this.bindOnboardingEvents(signal, resolve, reject);

                // Handle modal close event (e.g., clicking outside, pressing ESC)
                modal.addEventListener('close', () => {
                    // If the promise hasn't been resolved yet, it means the user cancelled
                    this.cleanupOnboardingListeners();
                    reject("Onboarding modal closed prematurely.");
                }, { signal });

            } catch (error) {
                console.error("Error showing or setting up onboarding modal:", error);
                this.cleanupOnboardingListeners();
                reject(error);
            }
        });
    }

    bindOnboardingEvents(signal, resolveOnboarding, rejectOnboarding) {
        const modal = document.getElementById('new-user-modal');
        if (!modal) return;

        // --- Screen 1: Username ---
        const usernameInput = document.getElementById('username-input');
        const usernameSuggestions = document.getElementById('username-suggestions');
        const usernameContinueBtn = document.getElementById('username-continue-button');

        usernameInput?.addEventListener('input', this.handleUsernameInput.bind(this), { signal });
        usernameInput?.addEventListener('blur', () => setTimeout(() => usernameSuggestions.classList.add('hidden'), 150), { signal }); // Hide on blur
        usernameContinueBtn?.addEventListener('click', this.handleUsernameContinue.bind(this), { signal });
        document.getElementById('username-suggestions-list')?.addEventListener('mousedown', this.handleSuggestionClick.bind(this), { signal }); // mousedown fires before blur

        // --- Screen 2: Profile ---
        const profileInputs = modal.querySelectorAll('#profile-screen input[required], #profile-screen select[required]');
        profileInputs.forEach(input => {
            input.addEventListener('input', this.validateProfileForm.bind(this), { signal });
            input.addEventListener('blur', this.handleInputBlur.bind(this), { signal });
        });
        document.getElementById('profile-back-button')?.addEventListener('click', () => {
            this.showScreen('username-screen');
            this.resetProfileForm();
        }, { signal });
        document.getElementById('profile-continue-button')?.addEventListener('click', this.handleProfileContinue.bind(this), { signal });

        // --- Screen 3: Customize ---
        const themeOptions = modal.querySelectorAll('.theme-option');
        themeOptions.forEach(option => {
            option.addEventListener('click', this.handleThemeSelection.bind(this), { signal });
        });
        // Avatar elements
        document.getElementById('onboarding-change-avatar-style')?.addEventListener('click', this.toggleOnboardingAvatarDropdown.bind(this), { signal });
        document.getElementById('onboarding-randomize-avatar')?.addEventListener('click', this.handleOnboardingRandomizeAvatar.bind(this), { signal });
        document.getElementById('onboarding-avatar-style-dropdown')?.addEventListener('click', this.handleOnboardingAvatarStyleSelection.bind(this), { signal });
        // Back/Finish buttons
        document.getElementById('customize-back-button')?.addEventListener('click', () => this.showScreen('profile-screen'), { signal });
        document.getElementById('finish-setup-button')?.addEventListener('click', () => this.handleFinishSetup(resolveOnboarding), { signal }); // Pass the resolve function

        // Initial setup for customize screen (if needed when shown)
        this.populateOnboardingAvatarOptions(signal); // Populate dropdown previews
        this.updateThemeSelectionUI(); // Set initial theme highlight
        this.updateOnboardingAvatarDisplay(); // Set initial avatar image
    }

    cleanupOnboardingListeners() {
        if (this.onboardingAbortController) {
            this.onboardingAbortController.abort();
            this.onboardingAbortController = null;
        }
    }

    resetOnboardingState() {
        this.selectedUserData = null;
        this.selectedTheme = this.context?.themeManger?.getTheme() || 'cool';
        this.selectedAvatarType = this.defaultAvatarType;
        this.selectedAvatarUrl = this.generateAvatarUrl(this.selectedAvatarType);
        this.cleanupOnboardingListeners(); // Ensure any previous attempt is cleaned
    }

    // --- Screen 1: Username Logic ---

    handleUsernameInput(event) {
        const input = event.target;
        const value = input.value.trim();
        const continueButton = document.getElementById('username-continue-button');
        const suggestionsContainer = document.getElementById('username-suggestions');

        continueButton.disabled = !value;
        continueButton.dataset.tooltip = value ? '' : 'Please enter a username';

        if (value.length > 1) { // Only search if length > 1
            this.showUsernameLoading(true);
            this.debouncedFetchUserSuggestions(value);
        } else {
            suggestionsContainer.classList.add('hidden');
            suggestionsContainer.querySelector('ul').innerHTML = ''; // Clear suggestions
            this.showUsernameLoading(false);
        }
        // Clear pre-selected data if user types again after selecting
        this.selectedUserData = null;
    }

    async fetchUserSuggestions(query) {
        const suggestionsList = document.getElementById('username-suggestions-list');
        const suggestionsContainer = document.getElementById('username-suggestions');
        if (!suggestionsList || !suggestionsContainer) return;

        suggestionsList.innerHTML = ''; // Clear previous

        try {
            const response = await fetch(`/api/users/phonebook/?query=${encodeURIComponent(query)}`);
            this.showUsernameLoading(false);

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();

            console.log(data)

            if (data.length > 0) {
                data.forEach(user => {
                    const userData = user;
                    const li = document.createElement('li');
                    li.className = 'px-3 py-2 hover:bg-base-300 cursor-pointer text-sm';
                    li.textContent = user.firstName ? `${user.firstName} ${user.lastName} (${user.username})` : user.username;
                    li.dataset.username = user.username;
                    li.dataset.userData = JSON.stringify(userData);
                    suggestionsList.appendChild(li);
                });
                suggestionsContainer.classList.remove('hidden');
            } else {
                suggestionsContainer.classList.add('hidden');
            }
        } catch (error) {
            console.error('Error fetching username suggestions:', error);
            this.showUsernameLoading(false);
            suggestionsContainer.classList.add('hidden');
        }
    }

    handleSuggestionClick(event) {
        const target = event.target.closest('li[data-username]');
        if (target) {
            const username = target.dataset.username;
            const userData = JSON.parse(target.dataset.userData); // Retrieve the stored data

            document.getElementById('username-input').value = username;
            document.getElementById('username-suggestions').classList.add('hidden');
            document.getElementById('username-continue-button').disabled = false;

            // Store the selected user's data
            this.selectedUserData = userData;
        }
    }

    async handleUsernameContinue() {
        const usernameInput = document.getElementById('username-input');
        const username = usernameInput.value.trim();
        const continueButton = document.getElementById('username-continue-button');

        if (!username) {
            this.showInputError(usernameInput);
            return;
        }

        continueButton.classList.add('btn-disabled', 'loading'); // Show loading state
        continueButton.disabled = true;

        try {

            if (this.selectedUserData) {
                this.prefillProfileForm();
            } else {
                this.selectedUserData = null;
                this.resetProfileForm(); // Ensure form is clear if no match
            }

            this.showScreen('profile-screen');
            this.validateProfileForm(); // Re-validate in case of pre-fill

        } catch (error) {
            console.error("Error verifying username on continue:", error);
            this.selectedUserData = null;
            this.resetProfileForm();
            this.showScreen('profile-screen');
        } finally {
            continueButton.classList.remove('btn-disabled', 'loading');
            continueButton.disabled = !usernameInput.value.trim();
        }
    }

    showUsernameLoading(isLoading) {
        document.getElementById('username-loading-spinner')?.classList.toggle('hidden', !isLoading);
        document.getElementById('username-search-icon')?.classList.toggle('hidden', isLoading);
    }

    // --- Screen 2: Profile Logic ---

    prefillProfileForm() {
        if (!this.selectedUserData) return;

        // Map API data keys to form input IDs (adjust keys based on your API response)
        const fieldMap = {
            firstName: 'firstname-input',
            lastName: 'lastname-input',
            email: 'email-input',
            nickname: 'nickname-input',
            region: 'region-input',
            role: 'role-input'
        };

        Object.entries(fieldMap).forEach(([dataKey, elementId]) => {
            const element = document.getElementById(elementId);
            if (element && this.selectedUserData[dataKey]) {
                element.value = this.selectedUserData[dataKey];
            }
        });
    }

    resetProfileForm() {
        const fields = ['firstname-input', 'lastname-input', 'email-input', 'nickname-input', 'region-input', 'role-input'];
        fields.forEach(id => {
            const element = document.getElementById(id);
            if(element) {
                if (element.tagName === 'SELECT') {
                    element.value = ""; // Reset select
                } else {
                    element.value = ""; // Reset input
                }
                element.classList.remove('input-error');
            }
        });
        this.validateProfileForm(); // Update button state
    }

    validateProfileForm() {
        const requiredFields = ['firstname-input', 'lastname-input', 'email-input', 'region-input', 'role-input'];
        let isValid = true;

        requiredFields.forEach(id => {
            const input = document.getElementById(id);
            if (input && !input.value.trim()) {
                isValid = false;
            } else if (input) {
            }
        });

        const emailInput = document.getElementById('email-input');
        if (emailInput && emailInput.value && !/\S+@\S+\.\S+/.test(emailInput.value)) {
            isValid = false;
            this.showInputError(emailInput, false);
        } else if (emailInput) {
            emailInput.classList.remove('input-error');
        }

        const continueButton = document.getElementById('profile-continue-button');
        if (continueButton) {
            continueButton.disabled = !isValid;
        }
        return isValid;
    }

    handleProfileContinue() {
        if (this.validateProfileForm()) {
            this.showScreen('customize-screen');
            this.updateOnboardingAvatarDisplay();
            this.updateThemeSelectionUI();
        } else {
            const requiredFields = ['firstname-input', 'lastname-input', 'email-input', 'region-input', 'role-input'];
            requiredFields.forEach(id => {
                const input = document.getElementById(id);
                if (input && !input.value.trim()) {
                    this.showInputError(input);
                }
            });
            const emailInput = document.getElementById('email-input');
            if (emailInput && emailInput.value && !/\S+@\S+\.\S+/.test(emailInput.value)) {
                this.showInputError(emailInput);
            }
            console.error("Profile form invalid");
        }
    }

    // --- Screen 3: Customize Logic ---
    handleThemeSelection(event) {
        const target = event.currentTarget;
        const theme = target?.dataset?.themeTarget;
        if (theme) {
            this.selectedTheme = theme;
            this.updateThemeSelectionUI();
            this.themeManger.applyTheme(theme, false, true);
        }
    }

    updateThemeSelectionUI() {
        const themeOptions = document.querySelectorAll('#customize-screen .theme-option');
        themeOptions.forEach(option => {
            option.classList.toggle('btn-active', option.dataset.themeTarget === this.selectedTheme);
            option.classList.toggle('btn-outline', option.dataset.themeTarget !== this.selectedTheme);
        });
    }

    populateOnboardingAvatarOptions(signal) {
        const optionsContainer = document.getElementById('onboarding-avatar-style-dropdown');
        if (!optionsContainer) return;

        const options = optionsContainer.querySelectorAll('.onboarding-avatar-style-option');

        options.forEach(option => {
            const type = option.dataset.type;
            const img = option.querySelector('img');
            if (img && type) {
                const tempProfileData = {
                    firstName: document.getElementById('firstname-input')?.value || 'C',
                    lastName: document.getElementById('lastname-input')?.value || 'Z',
                };
                img.src = this.generateAvatarUrl(type, null, tempProfileData); // Pass temp data
            }
        });
    }

    toggleOnboardingAvatarDropdown(event) {
        event?.preventDefault();
        event?.stopPropagation(); // Prevent closing immediately if part of dropdown itself
        const dropdown = document.getElementById('onboarding-avatar-style-dropdown');
        const button = document.getElementById('onboarding-change-avatar-style');

        if (dropdown && button) {
            if (document.activeElement === button || dropdown.contains(document.activeElement)) {
                // TODO
            } else {
            }
            this.populateOnboardingAvatarOptions();
        }
    }

    handleOnboardingAvatarStyleSelection(event) {
        const target = event.target.closest('.onboarding-avatar-style-option[data-type]');
        if (!target) return; // Clicked outside a valid option

        event.preventDefault();
        event.stopPropagation();

        const current = document.getElementById('current-avatar');
        this.selectedAvatarType = target.dataset.type;
        this.selectedAvatarUrl = current.src = target.getElementsByTagName('img')[0].src;
        current.dataset.type = target.dataset.type;

        this.updateOnboardingAvatarDisplay();

        const dropdownParent = document.getElementById('onboarding-change-avatar-style')?.closest('.dropdown');
        dropdownParent?.removeAttribute('open');
        document.activeElement?.blur();
    }

    handleOnboardingRandomizeAvatar(event) {
        event.preventDefault();
        const onboardingUserInfo = this.selectedUserData;
        this.selectedAvatarUrl = this.generateAvatarUrl(this.selectedAvatarType, null, onboardingUserInfo);
        this.updateOnboardingAvatarDisplay();
    }

    handleSettingsRandomizeAvatar(event) {
        this.updateSettingsAvatarDisplay();
    }

    updateOnboardingAvatarDisplay() {
        const avatarImg = document.getElementById('onboarding-current-avatar');
        if (avatarImg) {
            avatarImg.src = this.selectedAvatarUrl;
        }
    }

    updateSettingsAvatarDisplay() {
        const avatarImg = document.getElementById('current-avatar');
        if (avatarImg) {
            this.selectedAvatarUrl = this.generateAvatarUrl(this.selectedAvatarType);
            avatarImg.src = this.selectedAvatarUrl;
        }
    }

    createGuestProfile() {
        // Guest profile doesn't get persisted
        return {
            username: 'Guest',
            firstName: 'Guest',
            lastName: 'User',
            displayName: 'Guest User',
            email: '',
            region: '',
            role: 'guest',
            theme: 'cool',
            avatar: this.generateAvatarUrl('initials', null, { firstName: '?', lastName: '?' }),
            createdAt: new Date().toISOString(),
            fingerprint: this.fp,
            //sessionFingerprint: this.session_fp,
            isGuest: true
        };
    }

    // --- Final Step ---

    async handleFinishSetup(resolveOnboarding) {
        const finishButton = document.getElementById('finish-setup-button');
        finishButton.classList.add('btn-disabled', 'loading');
        finishButton.disabled = true;

        try {
            // Gather all data
            const username = document.getElementById('username-input').value.trim();
            const firstName = document.getElementById('firstname-input').value.trim();
            const lastName = document.getElementById('lastname-input').value.trim();
            const email = document.getElementById('email-input').value.trim();
            const nickname = document.getElementById('nickname-input').value.trim();
            const region = document.getElementById('region-input').value;
            const role = document.getElementById('role-input').value;

            if (!username || !firstName || !lastName || !email || !region || !role) {
                console.error("Missing required fields on final submission.");
                finishButton.classList.remove('btn-disabled', 'loading');
                finishButton.disabled = false;
                if (!firstName || !lastName || !email || !region || !role) {
                    this.showScreen('profile-screen');
                    this.handleProfileContinue(); // Trigger validation highlights
                } else if (!username) {
                    this.showScreen('username-screen');
                    this.showInputError(document.getElementById('username-input'));
                }
                return;
            }

            let displayName;
            if (nickname && nickname !== '') {
                displayName = `${nickname} ${lastName}`;
            } else {
                displayName = `${firstName} ${lastName}`;
            }

            const newProfile = {
                username: username,
                firstName: firstName,
                lastName: lastName,
                displayName: displayName,
                email: email,
                nickname: nickname,
                region: region,
                role: role,
                avatar: this.selectedAvatarUrl,
                createdAt: new Date().toISOString(),
                fingerprint: this.fp.visitorId,
                //sessionFingerprint: this.context.session_fp
            };

            this.userProfile$.update(newProfile);
            resolveOnboarding(newProfile);

        } catch (error) {
            console.error("Error during final profile creation:", error);
            finishButton.classList.remove('btn-disabled', 'loading');
            finishButton.disabled = false;
        }
    }


    // --- Utility and Helper Methods ---

    showScreen(screenId) {
        const screens = document.querySelectorAll('.modal-screen');
        let foundScreen = false;
        screens.forEach(screen => {
            if (screen.id === screenId) {
                screen.classList.remove('hidden');
                screen.classList.add('active');
                foundScreen = true;
            } else {
                screen.classList.add('hidden');
                screen.classList.remove('active');
            }
        });
        if (!foundScreen) {
            console.warn(`Screen with ID "${screenId}" not found.`);
        }
        document.querySelector('.modal-box')?.scrollTo(0, 0);
    }

    showInputError(inputElement, autoHide = true) {
        if (!inputElement) return;
        inputElement.classList.add('input-error');
        if (autoHide) {
            setTimeout(() => inputElement.classList.remove('input-error'), 820);
        }
    }

    handleInputBlur(event) {
        const input = event.target;
        if (input.hasAttribute('required') && !input.value.trim()) {
            input.classList.add('input-error');
        } else {
            input.classList.remove('input-error');
        }
        if (input.id === 'email-input' && /\S+@\S+\.\S+/.test(input.value)) {
            input.classList.remove('input-error');
        }
    }

    resetForm() {
        const modal = document.getElementById('new-user-modal');
        if (!modal) return;
        const inputs = modal.querySelectorAll('input, select');
        inputs.forEach(input => {
            if (input.tagName === 'SELECT') {
                input.value = '';
                if(input.querySelector('option[disabled][selected]')) {
                    input.value = input.querySelector('option[disabled][selected]').value;
                }
            } else {
                input.value = '';
            }
            input.classList.remove('input-error');
        });

        document.getElementById('username-continue-button')?.setAttribute('disabled', 'true');
        document.getElementById('profile-continue-button')?.setAttribute('disabled', 'true');
        document.getElementById('username-suggestions-list').innerHTML = '';
        document.getElementById('username-suggestions').classList.add('hidden');
        this.showUsernameLoading(false);
        this.resetOnboardingState();
        this.updateThemeSelectionUI();
        this.updateOnboardingAvatarDisplay();
    }

    async waitForLordIconReady(li) {
        if (!li || typeof li.playerInstance?.isReady === 'undefined') return;
        while (!li.playerInstance.isReady) {
            await new Promise(resolve => setTimeout(resolve, 50));
        }
        try {
            li.playerInstance.addEventListener("complete", () => {
                li.setAttribute('state', 'default');
                li.setAttribute('trigger', 'hover');
            });
        } catch (e) { console.warn("Could not add complete listener to lord-icon", e)}
    }

    generateAvatarUrl(type, seed = null, profileData = null, size=36) {
        const dataToUse = profileData || this.userProfile$.getValue();
        const firstName = dataToUse?.firstName || 'C';
        const lastName = dataToUse?.lastName || 'Z';

        if (!seed) {
            seed = Math.random().toString(36).substring(2, 15);
        }

        let url = `https://api.dicebear.com/9.x/${type}/svg`;
        const params = new URLSearchParams();
        params.append('seed', seed);
        params.append('size', `${size}`);
        params.append('radius', '50');

        if (type === 'initials') {
            const initials = `${firstName[0]}${lastName[0]}`.toUpperCase();
            params.set('seed', initials);
            params.append('backgroundColor', Math.floor(Math.random()*16777215).toString(16).padStart(6, '0'));
            params.append('fontSize', '45');
        }
        return `${url}?${params.toString()}`;
    }

    // --- Persistent Event Binding (Outside Onboarding Modal) ---
    bindPersistentEvents() {
        // Profile dropdown (avatar-menu)
        const profileButton = document.getElementById('profile-button');
        if (profileButton) {
            profileButton.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation(); // Prevent dropdown-close propagation (critical for daisyUI, native details/summary, etc.)
                this.populateSettingsModal();
                const settingsModal = document.getElementById('settings-modal');
                if (settingsModal && typeof settingsModal.showModal === 'function') {
                    settingsModal.showModal();
                } else {
                    console.error("Settings modal not found or is not a dialog element.");
                }

                // Close dropdown after opening
                const profileDropdown = document.getElementById('profile-dropdown');
                if (profileDropdown && profileDropdown.classList.contains('dropdown-open')) {
                    profileDropdown.classList.remove('dropdown-open');
                }
            });
        }
        // Prevent focus glitch if user mouses down
        profileButton?.addEventListener('mousedown', e => e.stopPropagation());

        // Save profile changes in settings modal
        const saveProfileButton = document.getElementById('save-profile');
        saveProfileButton?.addEventListener('click', async (e) => {
            e.preventDefault();
            await this.saveProfileChanges();
            document.getElementById('settings-modal')?.close();
            await this.context.page.socketManager().identifyUser()
        });

        document.getElementById('cancel-profile')?.addEventListener('click', (e) => {
            e.preventDefault();
            document.getElementById('settings-modal')?.close();
        });

        // Logout button
        const logoutButton = document.getElementById('logout-button');
        logoutButton?.addEventListener('click', async (e) => {
            e.preventDefault();
            await this.handleLogout();
        });

        this.bindSettingsModalEvents();
    }

    bindSettingsModalEvents() {
        const settingsModal = document.getElementById('settings-modal');
        if (!settingsModal) return;

        const settingsInputs = settingsModal.querySelectorAll('input[required], select[required]');
        settingsInputs.forEach(input => {
            input.addEventListener('input', () => this.validateSettingsForm()); // Assuming validateSettingsForm exists
            input.addEventListener('change', () => this.validateSettingsForm());
        });

        document.getElementById('change-avatar-style')?.addEventListener('click', this.toggleSettingsAvatarDropdown.bind(this));
        // document.getElementById('randomize-avatar')?.addEventListener('click', this.handleSettingsRandomizeAvatar.bind(this));
        document.getElementById('avatar-style-dropdown')?.addEventListener('click', this.handleSettingsAvatarStyleSelection.bind(this));

        const changeAvatarButton = document.getElementById('change-avatar');
        const currentAvatar = document.getElementById('current-avatar');
        changeAvatarButton?.addEventListener('click', (e) => {
            e.preventDefault();
            const type = currentAvatar?.dataset.type || 'bottts-neutral'; // Default if none selected
            const newAvatar = this.generateAvatarUrl(type, null, this.userProfile$.getValue());
            const currentAvatarImg = document.getElementById('current-avatar');
            if (currentAvatarImg) currentAvatarImg.src = newAvatar;
            currentAvatarImg.dataset.type = type;
            this._samples = false;
            this.populateSettingsAvatarOptions();
        });

        this.populateSettingsAvatarOptions();
    }


    // --- User Profile Data Access ---

    getCurrentUserFirstName() {
        return this.userProfile$.getValue().get('nickname') || this.userProfile$.getValue().get('firstName') || '';
    }

    getCurrentUserLastName() {
        return this.userProfile$.getValue().get('lastName') || '';
    }

    getCurrentUserName() {
        const first = this.getCurrentUserFirstName();
        const last = this.getCurrentUserLastName();
        return first ? `${first} ${last}`.trim() : 'Guest';
    }

    getRoles() {
        // TODO
        return ['admin']
    }

    // --- UI Updates (e.g., Navbar) ---
    async fetchSvgContent(url) {
        try {
            const response = await fetch(url);

            // Check if the request was successful (status code 200-299)
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            // Get the response body as plain text, as SVG is essentially XML
            return await response.text();
        } catch (error) {
            console.error("Error fetching SVG:", error);
            return null; // Or handle the error as appropriate
        }
    }

    _guessType(url) {
        const p = url.toString().split("/svg")?.[0]?.split(".x/")?.[1]
        if (p) return p
        return 'initials'
    }

    async loadProfileDisplay(profile) {
        try {
            if (!this.userProfile$.size()) {
                document.getElementById('user-name').textContent = 'Guest User';
                document.getElementById('user-email').textContent = '';
                document.getElementById('user-avatar').src = this.generateAvatarUrl('initials', null, {firstName:'C', lastName:'Z'});
                document.getElementById('user-avatar').closest('.avatar').style.opacity = '1';
                return;
            }

            document.getElementById('user-name').textContent = this.getCurrentUserName();
            document.getElementById('user-email').textContent = this.userProfile$.getValue().get('email') || '';
            const avatarImg = document.getElementById('user-avatar');
            const avatarContainer = avatarImg?.closest('.avatar');

            if (avatarImg && avatarContainer) {
                const tempImg = new Image();
                tempImg.onload = () => {
                    avatarImg.src = this.userProfile$.getValue().get('avatar') || this.generateAvatarUrl('initials');
                    avatarImg.dataset.type = this.userProfile$.getValue().get('avatarType') || this._guessType(avatarImg.src);
                    avatarContainer.style.opacity = '1';
                };
                tempImg.onerror = () => {
                    console.warn("Failed to load user avatar, using fallback.");
                    avatarImg.src = this.generateAvatarUrl('initials');
                    avatarImg.dataset.type = 'initials';
                    avatarContainer.style.opacity = '1';
                }
                tempImg.src = this.userProfile$.getValue().get('avatar');
                tempImg.dataset.type = this.userProfile$.getValue().get('avatarType') || this._guessType(tempImg.src);
            }
        } catch (error) {
            console.error('Error loading profile display:', error);
            document.getElementById('user-name').textContent = 'Guest User';
            document.getElementById('user-email').textContent = '';
            const avatarImg = document.getElementById('user-avatar');
            if (avatarImg) {
                avatarImg.src = this.generateAvatarUrl('initials', null, {firstName:'C', lastName:'Z'});
                avatarImg.dataset.type = 'initials'
            }
            const avatarContainer = avatarImg?.closest('.avatar');
            if(avatarContainer) avatarContainer.style.opacity = '1';
        }
    }

    // --- Logout ---

    async handleLogout() {
        const confirmModal = document.getElementById('logout-confirm-modal');
        if (!confirmModal || typeof confirmModal.showModal !== 'function') {
            console.warn("Logout confirmation modal not found. Logging out directly.");
            await this.performLogout();
            return;
        }

        confirmModal.showModal();
        const confirmButton = document.getElementById('confirm-logout');
        const cancelButton = confirmModal.querySelector('.btn-cancel'); // Assuming a cancel button

        const confirmHandler = async () => {
            await this.performLogout();
            cleanup();
        };

        const cancelHandler = () => {
            confirmModal.close();
            cleanup();
        };

        const cleanup = () => {
            confirmButton?.removeEventListener('click', confirmHandler);
            cancelButton?.removeEventListener('click', cancelHandler);
            confirmModal.removeEventListener('close', cancelHandler);
        };

        confirmButton?.addEventListener('click', confirmHandler, { once: true });
        cancelButton?.addEventListener('click', cancelHandler, { once: true });
        confirmModal.addEventListener('close', cancelHandler, { once: true });
    }

    async performLogout() {
        this.userProfile$.clear();
        await this.initFingerprint();
    }

    // --- Settings Modal Logic (Placeholders - adapt your existing logic) ---

    populateSettingsModal() {
        if (!this.userProfile$.getValue().size) return;

        const fields = {
            'settings-firstname': this.userProfile$.getValue().get('firstName'),
            'settings-lastname': this.userProfile$.getValue().get('lastName'),
            'user-email-input': this.userProfile$.getValue().get('email'),
            'settings-nickname': this.userProfile$.getValue().get('nickname'),
            'settings-username': this.userProfile$.getValue().get('username'),
            'settings-region': this.userProfile$.getValue().get('region'),
            'settings-role': this.userProfile$.getValue().get('role'),
            'current-avatar': this.userProfile$.getValue().get('avatar'),
            'avatar-type': this.userProfile$.getValue().get('avatarType'),
        };

        Object.entries(fields).forEach(([id, value]) => {
            const element = document.getElementById(id);
            if (element) {
                if (element.tagName === 'IMG') {
                    element.src = value || this.generateAvatarUrl('initials'); // Fallback
                } else {
                    element.value = value || '';
                }
            } else {
                console.warn(`Settings modal element with ID "${id}" not found.`);
            }
        });

        this.validateSettingsForm(); // Validate after populating
        this.populateSettingsAvatarOptions(); // Populate avatar choices in settings
    }

    toggleSettingsAvatarDropdown(event) {
        event?.preventDefault();
        event?.stopPropagation(); // Prevent closing immediately if part of dropdown itself
        const dropdown = document.getElementById('avatar-style-dropdown');
        const button = document.getElementById('change-avatar-style');

        if (dropdown && button) {
            if (document.activeElement === button || dropdown.contains(document.activeElement)) {
                // TODO
            } else {
            }
            this.populateSettingsAvatarOptions();
        }
    }

    async populateSettingsAvatarOptions() {
        if (this._samples) return;
        this._samples = true;

        const optionsContainer = document.getElementById('avatar-style-dropdown');
        if (!optionsContainer) return;
        const options = optionsContainer.querySelectorAll('.avatar-style-option');

        for (const option of options) {
            const type = option.dataset.type;
            const img = option.querySelector('img');
            if (img && type) {
                const tempProfileData = {
                    firstName: document.getElementById('firstname-input')?.value || 'C',
                    lastName: document.getElementById('lastname-input')?.value || 'Z',
                };
                img.src = this.generateAvatarUrl(type, null, tempProfileData); // Pass temp data
            }
        }
    }

    handleSettingsAvatarStyleSelection(event) {
        const target = event.target.closest('.avatar-style-option[data-type]');
        if (!target) return;

        event.preventDefault();
        event.stopPropagation();

        this.selectedAvatarType = target.dataset.type;
        const current_avatar = document.getElementById('current-avatar')
        current_avatar.src = target.getElementsByTagName('img')[0].src;
        current_avatar.dataset.type = target.dataset.type;

        // Update selection state
        // const options = document.querySelectorAll('#avatar-style-dropdown .avatar-style-option');
        // options.forEach(opt => opt.classList.remove('selected'));
        // target.classList.add('selected');

        // Close dropdown
        const dropdownParent = target.closest('.dropdown');
        dropdownParent?.removeAttribute('open');
        document.activeElement?.blur();
    }

    async saveProfileChanges() {
        if (!this.validateSettingsForm()) {
            console.warn("Settings form is invalid. Cannot save.");
            return;
        }

        const firstName = document.getElementById('settings-firstname').value.trim();
        const lastName = document.getElementById('settings-lastname').value.trim();
        const nickname = document.getElementById('settings-nickname').value.trim();
        const username = document.getElementById('settings-username').value.trim();

        let displayName;
        if (nickname && nickname !== '') {
            displayName = `${nickname} ${lastName}`;
        } else {
            displayName = `${firstName} ${lastName}`;
        }

        const current = document.getElementById('current-avatar');
        const updatedProfile = {
            ...this.userProfile$.asObject(),
            firstName: firstName,
            lastName: lastName,
            displayName: displayName,
            email: document.getElementById('user-email-input').value.trim(),
            nickname: nickname,
            region: document.getElementById('settings-region').value,
            role: document.getElementById('settings-role').value,
            username: username,
            avatar: current.src,
            avatarType: current?.dataset?.type,
        };

        this.userProfile$.update(updatedProfile)
        // // console.log("Profile changes saved.");
    }

    validateSettingsForm() {
        const requiredFields = [
            'settings-firstname',
            'settings-lastname',
            'user-email-input',
            'settings-region',
            'settings-role'
        ];
        let isValid = true;
        const saveButton = document.getElementById('save-profile');

        requiredFields.forEach(fieldId => {
            const field = document.getElementById(fieldId);
            if (!field) {
                console.warn(`Settings validation: Element ${fieldId} not found.`);
                return; // Skip if element doesn't exist
            }
            const value = field.value.trim();
            if (!value) {
                field.classList.add('input-error');
                isValid = false;
            } else {
                field.classList.remove('input-error');
            }
        });

        // Email validation
        const emailInput = document.getElementById('user-email-input');
        if (emailInput && emailInput.value && !/\S+@\S+\.\S+/.test(emailInput.value)) {
            emailInput.classList.add('input-error');
            isValid = false;
        } else if (emailInput) {
            emailInput.classList.remove('input-error');
        }

        if (saveButton) {
            saveButton.disabled = !isValid;
        }
        return isValid;
    }

    setCookieData(name, value, options = {}) {
        const cookieDefaults = { expires: 365 * 5, secure: window.location.protocol === 'https:', sameSite: 'Lax', path: '/' };
        const mergedCookieOptions = { ...cookieDefaults, ...options.cookieOptions }; // Merge any specific options

        // --- Special handling for 'userProfile' key ---
        if (name === 'userProfile') {
            if (value === null || value === undefined) {
                // If setting null/undefined profile, remove both LS and cookie
                this.removeCookieData('userProfile', options);
                return false;
            }
            try {
                // 1. Create MINIMAL object for cookie
                const minimalData = {
                    fp: value.fingerprint, // Device fingerprint
                    sfp: this.session_fp,
                    un: value.username,
                    dn: value.displayName
                };
                const minimalDataString = JSON.stringify(minimalData);
                Cookies.set('user_identity', minimalDataString, mergedCookieOptions);
                // // console.log("Saved full profile to LS, minimal to cookie 'user_identity'");

                return true;

            } catch (error) {
                console.error(`Failed to set CookieData for 'userProfile':`, error);
                return false;
            }
        } else {
            // --- Standard handling for other keys ---
            try {
                const processedValue = (value === null || value === undefined)
                    ? ''
                    : (typeof value === 'object' ? JSON.stringify(value) : String(value));

                if (processedValue === '') {
                    this.removeCookieData(name, options); // Remove on empty value
                    return false;
                }
                Cookies.set(name, processedValue, mergedCookieOptions);
                return true;
            } catch (error) { console.error(`Failed to set CookieData '${name}':`, error); return false; }
        }
    }

    getCookieData(name, options = {}) {
        // --- Special handling for 'userProfile' key ---
        if (name === 'userProfile') {
            try {
                const fullProfileString = localStorage.getItem('userProfile');
                if (fullProfileString) {
                    try {
                        return JSON.parse(fullProfileString); // Parse and return full profile
                    } catch (e) {
                        console.error("Failed to parse stored userProfile from localStorage:", e);
                        localStorage.removeItem('userProfile');
                        this.removeCookieData('userProfile'); // Remove both LS and cookie
                        return null;
                    }
                }
                return null; // Not found in localStorage
            } catch (error) {
                console.error("Error accessing localStorage for userProfile:", error);
                return null;
            }
        } else {
            // --- Standard handling for other keys ---
            const { parseJson = true } = options;
            let value = undefined;
            try {
                value = Cookies.get(name); // String or undefined
                if (value === undefined) return null; // Not found anywhere
                if (parseJson && typeof value === 'string') {
                    const trimmedValue = value.trim();
                    if ((trimmedValue.startsWith('{') && trimmedValue.endsWith('}')) || (trimmedValue.startsWith('[') && trimmedValue.endsWith(']'))) {
                        try { return JSON.parse(value); }
                        catch (e) {
                            console.error(e)
                        }
                    }
                }
                return value;
            } catch (error) { console.error(`Failed to get PermaData '${name}':`, error); return null; }
        }
    }

    getMinimalUserCookie() {
        try {
            const minimalDataString = Cookies.get('user_identity');
            if (minimalDataString) {
                return JSON.parse(minimalDataString); // fp, sfp, un, dn
            }
            return null;
        } catch (e) {
            console.error("Failed to get or parse minimal user cookie 'user_identity':", e);
            return null;
        }
    }

    removeCookieData(name, options = {}) {
        // --- Special handling for 'userProfile' key ---
        if (name === 'userProfile') {
            try {
                // Remove BOTH localStorage full profile and minimal cookie
                Cookies.remove('user_identity', { path: '/', ...options.cookieOptions });
                // // console.log("Removed full profile from LS and minimal cookie 'user_identity'");
                return true;
            } catch (error) {
                console.error("Failed to remove PermaData for 'userProfile':", error);
                return false;
            }
        } else {
            // --- Standard handling for other keys ---
            try {
                Cookies.remove(name, { path: '/', ...options.cookieOptions });
                return true;
            } catch (error) { console.error(`Failed to remove PermaData '${name}':`, error); return false; }
        }
    }


    checkCookieData(name, options = {}) {
        const { detailed = false } = options;
        try {
            const cookieExists = Cookies.get(name) !== undefined;
            const exists = cookieExists;
            if (detailed) {
                return { exists, cookie: cookieExists };
            }
            return { exists };
        } catch (error) { console.error('Failed to check data existence:', error); return { exists: false }; }
    }

}

export default UserManager;
