# How to Run This Project

### 1. Run Locally (On Your Own Machine)
1. Open your terminal inside this project folder and install the required tools:
   ```bash
   npm install
   ```
2. Start the backend game server:
   ```bash
   npm start
   ```
3. Double-click the `index.html` file to open your tester dashboard in any web browser.

---

### 2. Connect Other Laptops (Over the Same Wi-Fi)
1. Open `server.js`, scroll to the bottom, and change the host from `'localhost'` to `'0.0.0.0'`:
   ```javascript
   server.listen(3000, '0.0.0.0', () => { ... });
   ```
2. Find your laptop's IP address by opening your command prompt and typing `ipconfig` (look for your **IPv4 Address**, e.g., `192.168.1.15`).
3. Send the `index.html` file to the other laptop, open it, and update the connection link to use your IP:
   ```javascript
   const socket = new WebSocket('ws://YOUR_LAPTOP_IP:3000');
   ```

---

### 3. Connect Other Laptops (Over the Internet)
1. Download **Ngrok** from [ngrok.com](https://ngrok.com).
2. Run this command in your terminal to create a public internet link:
   ```bash
   ngrok http 3000
   ```
3. Copy the secure public `wss://` address link that Ngrok gives you, and paste it into the `index.html` connection file on the other player's laptop.
