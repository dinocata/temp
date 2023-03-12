import CoreData
import RxSwift

protocol AbstractStorage {
    associatedtype DomainType: PersistableModel
    
    /// Retrieves an entity by its id.
    func findById(_ id: String) -> Observable<DomainType?>
    
    /// Returns the fist entity that matches the specified predicate.
    ///
    /// - Parameter predicate: Predicate to be matched with
    /// - Returns: Single entity
    func findFirst(with predicate: NSPredicate?) -> Observable<DomainType?>
    
    /// Finds all entities that match the specified predicate and sorts them accordingly.
    ///
    /// - Parameters:
    ///   - predicate: Predicate to be matched with
    ///   - sortDescriptors: Sorting rules
    /// - Returns: List of entities
    func findAll(with predicate: NSPredicate?,
                 sortDescriptors: [NSSortDescriptor]) -> Observable<[DomainType]>
    
    /// Creates or updates an entity to the local persistence and marks it for syncing.
    ///
    /// - Parameter entity: Entity to be saved
    func save(entity: DomainType) -> Single<DomainType>
    
    /// Creates or updates entities to the local persistence and marks it for syncing.
    ///
    /// - Parameter entities: Entities to be saved
    func save(entities: [DomainType]) -> Single<Void>
    
    /// Sets a softDeleted flag on an entity and marks it for syncing.
    /// This entity remains in the local persistence
    /// as long as still exists on the server.
    ///
    /// - Parameter entity: Entity to be soft deleted
    func softDelete(entity: DomainType) -> Single<Void>
    
    /// Performs soft delete on a list of entities.
    ///
    /// - Parameter entities: Entities to be soft deleted.
    func softDelete(entities: [DomainType]) -> Single<Void>
    
    /// Permanently deletes specified entity from local persistence.
    ///
    /// - Parameter entity: Entity to be deleted.
    func delete(entity: DomainType) -> Single<Void>
    
    /// Permanently deletes specified entities from local persistence.
    ///
    /// - Parameter entities: Entities to be deleted
    func delete(entities: [DomainType]) -> Single<Void>
    
    /// Permanently deletes all objects of this Domain type from local persistence.
    func deleteAll() -> Single<Void>
    
    /// Executes synchronous thread-safe context execution and returns a single optional value. Used for safely fetching data from a background context.
    func performContextExecutionSynchronized<T>(_ block: @escaping (_ context: NSManagedObjectContext) -> T?) -> T?
    
    /// Executes synchronous thread-safe context execution and returns an array. Used for safely fetching data from a background context.
    func performContextExecutionSynchronizedList<T>(_ block: @escaping (_ context: NSManagedObjectContext) -> [T]) -> [T]
    
    /// Executes asynchronous thread safe context block. Used for safely fetching data from a background context.
    func performContextExecution<T>(_ block: @escaping (_ context: NSManagedObjectContext) -> T) -> Single<T>
    
    /// Executes and saves any context changes in a single synchronous operation queue. Used for safely storing data on a background context.
    func performContextModification<T>(_ block: @escaping (_ context: NSManagedObjectContext) -> T) -> Single<T>
    
    /// Triggers an associated notification for this data type.
    /// Useful for reacting to changes on data update.
    func notify()
}

extension AbstractStorage {
    func findAll(with predicate: NSPredicate?, sortDescriptors: [NSSortDescriptor] = DomainType.CoreDataType.defaultSortDescriptor) -> Observable<[DomainType]> {
        return findAll(with: predicate, sortDescriptors: sortDescriptors)
    }
}

class Storage<T: PersistableModel>: AbstractStorage where T == T.CoreDataType.DomainType {
    
    let coreDataManager: CoreDataManager
    let coreDataStack: CoreDataStack
    let notificationManager: NotificationManager
    let errorLogger: ErrorLogger
    
    var changeNotifier: Observable<Void> {
        return notificationManager.observe(T.notificationName)
            .share()
            .throttle(.milliseconds(100), scheduler: MainScheduler.instance)
            .mapToVoid()
            .startWith(())
    }
    
    func getThreadBasedContext() -> NSManagedObjectContext {
        if Thread.isMainThread {
            return self.coreDataStack.mainContext
        }
        return self.coreDataStack.newBackgroundContext()
    }
    
    init(
        coreDataManager: CoreDataManager,
        coreDataStack: CoreDataStack,
        notificationManager: NotificationManager,
        errorLogger: ErrorLogger
    ) {
        self.coreDataManager = coreDataManager
        self.coreDataStack = coreDataStack
        self.notificationManager = notificationManager
        self.errorLogger = errorLogger
    }
    
    func findById(_ identifier: String) -> Observable<T?> {
        return changeNotifier.map { self.findById(identifier) }
    }
    
    func findFirst(with predicate: NSPredicate?) -> Observable<T?> {
        return changeNotifier.map { self.findFirst(with: predicate) }
    }
    
    func findAll(with predicate: NSPredicate?, sortDescriptors: [NSSortDescriptor]) -> Observable<[T]> {
        return changeNotifier.map { self.findAll(with: predicate, sortDescriptors: sortDescriptors) }
    }
    
    func save(entity: T) -> Single<T> {
        return performContextModification { context in
            let object = self.coreDataManager.saveDomain(entity, context: context)
            object.synced = false
            object.softDeleted = false
            object.dateUpdated = Date()
            return entity
        }
    }
    
    func save(entities: [T]) -> Single<Void> {
        return performContextModification { context in
            entities.forEach {
                let object = self.coreDataManager.saveDomain($0, context: context)
                object.synced = false
                object.softDeleted = false
                object.dateUpdated = Date()
            }
        }
    }
    
    func softDelete(entity: T) -> Single<Void> {
        return performContextModification { context in
            if let object = self.coreDataManager.findByDomain(entity, context: context) {
                object.softDeleted = true
                object.synced = false
                object.dateUpdated = Date()
            }
        }
    }
    
    func softDelete(entities: [T]) -> Single<Void> {
        return performContextModification { context in
            entities
                .compactMap { self.coreDataManager.findByDomain($0, context: context) }
                .forEach {
                    $0.softDeleted = true
                    $0.synced = false
                    $0.dateUpdated = Date()
                }
        }
    }
    
    func delete(entity: T) -> Single<Void> {
        return performContextModification { context in
            if let object = self.coreDataManager.findByDomain(entity, context: context) {
                self.coreDataManager.delete(object, context: context)
            }
        }
    }
    
    func delete(entities: [T]) -> Single<Void> {
        return performContextModification { context in
            entities
                .compactMap { self.coreDataManager.findByDomain($0, context: context) }
                .forEach { self.coreDataManager.delete($0, context: context) }
        }
    }
    
    func deleteAll() -> Single<Void> {
        return performContextModification { context in
            self.coreDataManager.deleteAllEntities(entity: T.CoreDataType.self, context: context)
        }
    }
    
    func findById(_ identifier: String) -> T? {
        return performContextExecutionSynchronized { context in
            self.coreDataManager
                .findById(T.CoreDataType.self, id: identifier, context: context)?
                .asDomain
        }
    }
    
    func findFirst(with predicate: NSPredicate?) -> T? {
        return performContextExecutionSynchronized { context in
            let compoundPredicate = self.buildPredicate(predicate: predicate)
            return self.coreDataManager
                .findFirst(T.CoreDataType.self, predicate: compoundPredicate, context: context)?
                .asDomain
        }
    }
    
    func findAll(with predicate: NSPredicate?, sortDescriptors: [NSSortDescriptor]) -> [T] {
        return performContextExecutionSynchronizedList { context in
            let compoundPredicate = self.buildPredicate(predicate: predicate)
            return self.coreDataManager
                .findAll(T.CoreDataType.self,
                         sortDescriptors: sortDescriptors,
                         predicate: compoundPredicate,
                         context: context)
                .map(\.asDomain)
        }
    }
    
    private func buildPredicate(predicate: NSPredicate?) -> NSPredicate {
        var predicates = [NSPredicate(format: "softDeleted = %d", false)]
        if let predicate = predicate { predicates.append(predicate) }
        return NSCompoundPredicate(type: .and, subpredicates: predicates)
    }
    
    func performContextExecutionSynchronized<T>(_ block: @escaping (NSManagedObjectContext) -> T?) -> T? {
        var data: T?
        let context = self.getThreadBasedContext()
        context.performAndWait {
            data = block(context)
        }
        return data
    }
    
    func performContextExecutionSynchronizedList<T>(_ block: @escaping (NSManagedObjectContext) -> [T]) -> [T] {
        var data = [T]()
        let context = self.getThreadBasedContext()
        context.performAndWait {
            data = block(context)
        }
        return data
    }
    
    func performContextExecution<T>(_ block: @escaping (_ context: NSManagedObjectContext) -> T) -> Single<T> {
        return .create { observer in
            let context = self.getThreadBasedContext()
            context.perform {
                observer(.success(block(context)))
            }
            return Disposables.create()
        }
    }
    
    func performContextModification<T>(_ block: @escaping (_ context: NSManagedObjectContext) -> T) -> Single<T> {
        return .create { observer in
            self.coreDataStack.enqueue { context in
                let data = block(context)
                self.coreDataStack.save(context: context)
                self.coreDataStack.saveMainContext()
                self.notify()
                observer(.success(data))
            }
            return Disposables.create()
        }
    }
    
    func notify() {
        DispatchQueue.main.async {
            self.notificationManager.post(T.notificationName)
        }
    }
}
