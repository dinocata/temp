import CoreData

/// This helper provides instance to Core Data managed object context.
// sourcery: injectable = CoreDataStackImpl, singleton
protocol CoreDataStack {
    /// Should use for read-only operations. Never save a context directly.
    var mainContext: NSManagedObjectContext { get }
    
    func newBackgroundContext() -> NSManagedObjectContext
    
    /// Performs thread-safe write operation on a background context and saves it to the persistent store.
    /// Never save a context directly unless you know what you're doing. Even then, you should avoid doing it for consistency.
    /// https://stackoverflow.com/a/42745378
    /// - Parameter block: Returns an operation block to perform for a context
    func enqueue(block: @escaping (_ context: NSManagedObjectContext) -> Void)
    
    func saveMainContext()
    func save(context: NSManagedObjectContext)
}

// MARK: - Core Data stack
final class CoreDataStackImpl: CoreDataStack {
    
    static let defaultMergePolicy = NSMergeByPropertyObjectTrumpMergePolicy
    
    var mainContext: NSManagedObjectContext {
        self.persistentContainer.viewContext
    }
    
    private lazy var persistentContainer: NSPersistentContainer = {
        /*
         The persistent container for the application. This implementation
         creates and returns a container, having loaded the store for the
         application to it. This property is optional since there are legitimate
         error conditions that could cause the creation of the store to fail.
         */
        var container = NSPersistentContainer(name: AppConfig.CoreData.persistentContainerName)
        
        do {
            container = try migrationManager.performMigrations(for: container)
        } catch {
            errorLogger.logError(error)
        }
        
        container.loadPersistentStores(completionHandler: { _, error in
            
            container.viewContext.mergePolicy = Self.defaultMergePolicy
            
            if let error = error as NSError? {
                /*
                 Typical reasons for an error here include:
                 * The parent directory does not exist, cannot be created, or disallows writing.
                 * The persistent store is not accessible, due to permissions or data protection when the device is locked.
                 * The device is out of space.
                 * The store could not be migrated to the current model version.
                 Check the error message to determine what the actual problem was.
                 */
                self.errorLogger.logError(error)
            }
        })
        return container
    }()
    
    private lazy var persistentContainerQueue: OperationQueue = {
        let operationQueue = OperationQueue()
        operationQueue.maxConcurrentOperationCount = 1
        return operationQueue
    }()
    
    // MARK: Dependencies
    private let migrationManager: MigrationManager
    private let errorLogger: ErrorLogger
    
    init(migrationManager: MigrationManager,
         errorLogger: ErrorLogger) {
        self.migrationManager = migrationManager
        self.errorLogger = errorLogger
    }
    
    func newBackgroundContext() -> NSManagedObjectContext {
        self.persistentContainer.newBackgroundContext()
    }
    
    func enqueue(block: @escaping (_ context: NSManagedObjectContext) -> Void) {
        self.persistentContainerQueue.addOperation {
            let context: NSManagedObjectContext = self.newBackgroundContext()
            
            context.mergePolicy = Self.defaultMergePolicy
            context.performAndWait {
                block(context)
            }
        }
    }
    
    func saveMainContext() {
        self.save(context: mainContext)
    }
    
    func save(context: NSManagedObjectContext) {
        if context.hasChanges {
            context.performAndWait {
                do {
                    try context.save()
                } catch {
                    self.errorLogger.logError(error as NSError)
                }
            }
        }
    }
}

/// Provides instance to Core Data managed object context used for TESTING.
/// Unlike normal Core Data context, this instance is only persistent while App is active.
final class MockCoreDataStack: CoreDataStack {
    
    var mainContext: NSManagedObjectContext {
        self.mockPersistentContainer.viewContext
    }
    
    private lazy var mockPersistentContainer: NSPersistentContainer = {
        
        let container = NSPersistentContainer(name: AppConfig.CoreData.persistentContainerName, managedObjectModel: self.managedObjectModel)
        let description = NSPersistentStoreDescription()
        description.type = NSInMemoryStoreType
        description.shouldAddStoreAsynchronously = false // Make it simpler in test env
        
        container.persistentStoreDescriptions = [description]
        container.loadPersistentStores { description, error in
            // Check if the data store is in memory
            precondition( description.type == NSInMemoryStoreType )
            
            container.viewContext.mergePolicy = CoreDataStackImpl.defaultMergePolicy
            
            if let error = error {
                self.errorLogger.logError(error)
            }
        }
        return container
    }()
    
    lazy var managedObjectModel: NSManagedObjectModel = {
        // The managed object model for the application. This property is not optional. It is a fatal error for the application not to be able to find and load its model.
        let modelURL = Bundle.main.url(forResource: AppConfig.CoreData.persistentContainerName, withExtension: "momd")!
        return NSManagedObjectModel(contentsOf: modelURL)!
    }()
    
    private let errorLogger: ErrorLogger
    
    init(errorLogger: ErrorLogger) {
        self.errorLogger = errorLogger
    }
    
    func newBackgroundContext() -> NSManagedObjectContext {
        // Do not use background contexts in tests
        self.mainContext
    }
    
    /// We can and should always perform tests on the main thread
    func enqueue(block: @escaping (NSManagedObjectContext) -> Void) {
        let context = self.mainContext
        context.performAndWait {
            block(context)
        }
    }
    
    func saveMainContext() {
        self.save(context: mainContext)
    }
    
    func save(context: NSManagedObjectContext) {
        if context.hasChanges {
            context.performAndWait {
                do {
                    try context.save()
                } catch {
                    self.errorLogger.logError(error as NSError)
                }
            }
        }
    }
}
