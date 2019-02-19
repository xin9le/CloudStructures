using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

// internal fast type helper

namespace CloudStructures
{
    internal interface IMemberAccessor
    {
        string Name { get; }
        Type DeclaringType { get; }
        Type MemberType { get; }
        bool IsReadable { get; }
        bool IsWritable { get; }

        object GetValue(object target);
        void SetValue(object target, object value);
    }

    internal interface ITypeAccessor : IReadOnlyDictionary<string, IMemberAccessor>
    {
        object CreateNew();
        bool CanCreateNew { get; }
        Type DeclaringType { get; }
    }

    internal static class TypeAccessor
    {
        static readonly ConcurrentDictionary<Type, ITypeAccessor> cache = new ConcurrentDictionary<Type, ITypeAccessor>();

        public static ITypeAccessor Lookup(Type targetType)
        {
            return cache.GetOrAdd(targetType, t =>
            {
                var isAnonymousType = IsAnonymousType(t);

                var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetProperty | BindingFlags.SetProperty)
                    .Where(x => !x.GetCustomAttributes(typeof(CompilerGeneratedAttribute), true).Any())
                    .Select(pi => new ExpressionMemberAccessor(pi));

                var fields = t.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetField | BindingFlags.SetField)
                    .Where(_ => !isAnonymousType)
                    .Where(x => !x.GetCustomAttributes(typeof(CompilerGeneratedAttribute), true).Any()) // ignore backing field
                    .Select(fi => new ExpressionMemberAccessor(fi));

                var dict = new ReadOnlyDictionary<string, IMemberAccessor>(props.Concat(fields).ToDictionary(x => x.Name, x => (IMemberAccessor)x));
                return new ExpressionTypeAccessor(t, dict);
            });
        }

        static bool IsAnonymousType(Type type)
        {
            return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                && type.IsGenericType && type.Name.Contains("AnonymousType")
                && (type.Name.StartsWith("<>", StringComparison.OrdinalIgnoreCase) ||
                    type.Name.StartsWith("VB$", StringComparison.OrdinalIgnoreCase))
                && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }

        abstract class TypeAccessorBase : ITypeAccessor
        {
            protected readonly Type declaringType;
            protected readonly ReadOnlyDictionary<string, IMemberAccessor> members;

            public TypeAccessorBase(Type declaringType, ReadOnlyDictionary<string, IMemberAccessor> members)
            {
                this.declaringType = declaringType;
                this.members = members;
            }

            public abstract object CreateNew();

            public abstract bool CanCreateNew { get; }

            public Type DeclaringType
            {
                get { return declaringType; }
            }

            public bool ContainsKey(string key)
            {
                return members.ContainsKey(key);
            }

            public IEnumerable<string> Keys
            {
                get { return members.Keys; }
            }

            public bool TryGetValue(string key, out IMemberAccessor value)
            {
                return members.TryGetValue(key, out value);
            }

            public IEnumerable<IMemberAccessor> Values
            {
                get { return members.Values; }
            }

            public IMemberAccessor this[string key]
            {
                get { return members[key]; }
            }

            public int Count
            {
                get { return members.Count; }
            }

            public IEnumerator<KeyValuePair<string, IMemberAccessor>> GetEnumerator()
            {
                return members.GetEnumerator();
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return members.GetEnumerator();
            }
        }

        class ExpressionTypeAccessor : TypeAccessorBase
        {
            readonly Func<object> createNew;

            public ExpressionTypeAccessor(Type declaringType, ReadOnlyDictionary<string, IMemberAccessor> members)
                : base(declaringType, members)
            {
                if (declaringType.IsClass)
                {
                    var constructorInfo = declaringType.GetConstructor(Type.EmptyTypes);
                    if (constructorInfo == null) return;
                }

                if (!declaringType.IsValueType)
                {
                    var lambda = Expression.Lambda<Func<object>>(Expression.New(declaringType));
                    createNew = lambda.Compile();
                }
                else
                {
                    var lambda = Expression.Lambda<Func<object>>(Expression.Convert(Expression.New(declaringType), typeof(object)));
                    createNew = lambda.Compile();
                }
            }

            public override object CreateNew()
            {
                if (createNew == null) throw new InvalidOperationException(@"Type doesn't have no zero argument constructor.");

                return createNew();
            }

            public override bool CanCreateNew
            {
                get { return createNew != null; }
            }
        }

        class ExpressionMemberAccessor : IMemberAccessor
        {
            public Type DeclaringType { get; private set; }
            public Type MemberType { get; private set; }
            public string Name { get; private set; }
            public bool IsReadable { get { return GetValueDirect != null; } }
            public bool IsWritable { get { return SetValueDirect != null; } }

            // for performance optimization
            public readonly Func<object, object> GetValueDirect;
            public readonly Action<object, object> SetValueDirect;

            public ExpressionMemberAccessor(PropertyInfo info)
            {
                this.Name = info.Name;
                this.DeclaringType = info.DeclaringType;
                this.MemberType = info.PropertyType;
                this.GetValueDirect = (info.GetGetMethod(false) != null) ? CreateGetValue(DeclaringType, Name) : null;
                this.SetValueDirect = (info.GetSetMethod(false) != null) ? CreateSetValue(DeclaringType, Name) : null;
            }

            public ExpressionMemberAccessor(FieldInfo info)
            {
                this.Name = info.Name;
                this.DeclaringType = info.DeclaringType;
                this.MemberType = info.FieldType;
                this.GetValueDirect = CreateGetValue(DeclaringType, Name);
                this.SetValueDirect = (!info.IsInitOnly) ? CreateSetValue(DeclaringType, Name) : null;
            }

            public object GetValue(object target)
            {
                return GetValueDirect(target);
            }

            public void SetValue(object target, object value)
            {
                SetValueDirect(target, value);
            }

            // (object x) => (object)((T)x).name
            static Func<object, object> CreateGetValue(Type type, string name)
            {
                var x = Expression.Parameter(typeof(object), "x");

                var func = Expression.Lambda<Func<object, object>>(
                    Expression.Convert(
                        Expression.PropertyOrField(
                            (type.IsValueType ? Expression.Unbox(x, type) : Expression.Convert(x, type)),
                            name),
                        typeof(object)),
                    x);

                return func.Compile();
            }

            // (object x, object v) => ((T)x).name = (U)v
            static Action<object, object> CreateSetValue(Type type, string name)
            {
                var x = Expression.Parameter(typeof(object), "x");
                var v = Expression.Parameter(typeof(object), "v");

                var left = Expression.PropertyOrField(
                    (type.IsValueType ? Expression.Unbox(x, type) : Expression.Convert(x, type)),
                    name);
                var right = Expression.Convert(v, left.Type);

                var action = Expression.Lambda<Action<object, object>>(
                    Expression.Assign(left, right),
                    x, v);

                return action.Compile();
            }
        }
    }
}