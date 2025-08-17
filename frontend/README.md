# Trading Data Admin Control Panel

A modern Angular-based admin interface for managing trading data operations, built with Angular 20, DaisyUI, and Tailwind CSS.

## 🚀 Quick Start

### Prerequisites

- Node.js (v18 or higher)
- Angular CLI (`npm install -g @angular/cli`)
- Backend API running on `http://localhost:8002`

### Installation & Setup

1. **Install dependencies:**

   ```bash
   cd frontend
   npm install
   ```

2. **Start the development server:**

   ```bash
   ng serve
   ```

3. **Access the admin panel:**
   Open your browser and navigate to `http://localhost:4200/admin`

## 📊 Current Features

### ✅ Fully Implemented & Working

- **Trading Data Management**

  - Real-time symbol loading from backend storage
  - On-demand data updates with progress tracking
  - Data retrieval with filtering (symbol, timeframe, date range)
  - Support for multiple timeframes (1min, 5min, 15min, 30min, 1h, 2h, 4h, daily)

- **User Interface**

  - Responsive design with DaisyUI components
  - Real-time progress indicators
  - Error handling with user-friendly messages
  - Symbol dropdown with available stored symbols
  - Data table with OHLCV display

- **Backend Integration**
  - Auto-generated OpenAPI client from backend schema
  - Full API integration with error handling
  - Timezone-aware data processing
  - CORS-enabled communication

### 🔧 Technical Implementation

- **Framework**: Angular 20 with standalone components
- **Styling**: Tailwind CSS + DaisyUI
- **HTTP Client**: Auto-generated from OpenAPI schema
- **State Management**: RxJS Observables
- **Testing**: Jasmine/Karma with comprehensive test coverage

## 🛠 Development

### Available Scripts

```bash
# Development server
ng serve

# Run tests
ng test

# Build for production
ng build --configuration production

# Generate API client from backend
npm run generate-api

# Linting and formatting
ng lint
```

### API Integration

The frontend uses auto-generated services from the backend's OpenAPI schema:

- **TradingDataService**: Handles data operations
- **AdminService**: Wraps trading data operations for admin use
- **Models**: Type-safe interfaces matching backend models

### Environment Configuration

- **Development**: `src/environments/environment.ts`

  - API URL: `http://localhost:8002`
  - Debug mode enabled

- **Production**: `src/environments/environment.prod.ts`
  - Configure production API URL
  - Debug mode disabled

## 🏗 Architecture

### Component Structure

```
src/app/
├── admin/                          # Admin panel module
│   ├── admin-control-panel.component.* # Main admin interface
│   ├── admin.service.ts            # Admin operations service
│   ├── admin.guard.ts              # Route protection (TODO: implement auth)
│   └── error-handler.interceptor.ts # HTTP error handling
├── api/                            # Auto-generated OpenAPI client
│   ├── models/                     # Type definitions
│   ├── services/                   # HTTP services
│   └── fn/                         # Function-based API calls
└── app.config.ts                   # Application configuration
```

### Data Flow

1. **Component** → **AdminService** → **TradingDataService** → **Backend API**
2. **Backend API** → **Observable** → **Component** → **UI Update**

## 🔧 Troubleshooting

### Common Issues

1. **API Connection Errors:**

   - Verify backend is running on `http://localhost:8002`
   - Check browser Network tab for failed requests
   - Ensure CORS is properly configured on backend

2. **Symbol Loading Issues:**

   - Check if backend has stored trading data
   - Verify `/trading-data/symbols` endpoint returns data
   - Look for timezone-related errors in backend logs

3. **Build Errors:**
   - Run `npm install` to ensure dependencies are current
   - Check Angular and Node.js version compatibility
   - Clear `node_modules` and reinstall if needed

### Development Tips

1. **Use Browser DevTools** to inspect API calls and responses
2. **Check Console logs** for detailed error messages
3. **Test API endpoints directly** using Swagger UI at `http://localhost:8002/docs`
4. **Monitor backend logs** for server-side errors

## 🚀 Production Deployment

### Build Process

```bash
# Production build
ng build --configuration production

# Output location
dist/frontend/
```

### Security Considerations

- **Authentication**: Implement proper JWT-based authentication
- **HTTPS**: Use HTTPS in production environments
- **CORS**: Configure restrictive CORS policies for production
- **Rate Limiting**: Implement API rate limiting
- **Audit Logging**: Log all admin actions for security auditing

## 🧪 Testing

### Running Tests

```bash
# Unit tests
ng test

# Run tests in CI mode
ng test --watch=false --browsers=ChromeHeadless

# Test coverage
ng test --code-coverage
```

### Test Coverage

- **Components**: Full test coverage with mocked services
- **Services**: Integration tests with HTTP client mocking
- **Models**: Type safety validation
- **Error Handling**: Comprehensive error scenario testing

## 📝 Recent Updates

### Latest Changes (Current Development)

- ✅ **Fixed timezone handling** in data storage service
- ✅ **Implemented real backend integration** replacing mock data
- ✅ **Added comprehensive error handling** for API failures
- ✅ **Enhanced symbol management** with auto-refresh functionality
- ✅ **Updated API configuration** for proper backend communication
- ✅ **Fixed test suite** with proper service mocking

### Next Steps

1. **Implement authentication system** using AdminGuard
2. **Add data export functionality** (CSV, JSON formats)
3. **Enhance filtering capabilities** with advanced search
4. **Add real-time updates** using WebSocket connections
5. **Implement audit logging** for admin actions
6. **Add data visualization** charts and graphs
